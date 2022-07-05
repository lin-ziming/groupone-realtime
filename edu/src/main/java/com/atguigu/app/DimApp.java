package com.atguigu.app;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.Constant;
import com.atguigu.util.FlinkSinkUtil;
import com.atguigu.util.FlinkSourceUtil;
import com.atguigu.util.JdbcUtil;
import com.google.common.base.Strings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;


/**
 * @author shogunate
 * @description DimApp: Sourcing FlinkCDC filter dim table from ods_db.
 * @date 2022/7/4 15:07
 */
public class DimApp extends BaseAppV1{

    public static void main(String[] args) {
        new DimApp().init(11041, 2, "DimApp", Constant.TOPIC_ODS_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //etl
        SingleOutputStreamOperator<String> etlStream = etl(stream);

        //flinkCDC source table config
        SingleOutputStreamOperator<TableProcess> tpStream = tpSourceFromFlinkCDC(env);

        //Create sinkTable into phoenix and connect dataStream
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connect(etlStream, tpStream);

        //filter sink cols
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> sinkStream = filterSinkCols(connectedStream);

        //write to pho
        sinkStream.addSink(FlinkSinkUtil.getPhoenixSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterSinkCols(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        return stream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {

                JSONObject data = value.f0;
                TableProcess tp = value.f1;
                Set<String> dataCols = data.keySet();


                List<String> columns = Arrays.asList(tp.getSinkColumns().split(","));

                dataCols.removeIf(col->!columns.contains(col));

                return value;
            }
        });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<String> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        MapStateDescriptor<String, TableProcess> bcMapStateDesc = new MapStateDescriptor<>("broadcastMapState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(bcMapStateDesc);
        return dataStream.connect(bcStream)
            .process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                private Connection phoenixConn;

                @Override
                public void open(Configuration parameters) throws Exception {
                    phoenixConn = JdbcUtil.getPhoenixConnection();
                }

                @Override
                public void close() throws Exception {
                    if (phoenixConn != null) {
                        phoenixConn.close();
                    }
                }

                @Override
                public void processElement(String value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    ReadOnlyBroadcastState<String, TableProcess> readTpBcState = ctx.getBroadcastState(bcMapStateDesc);
                    JSONObject data = JSONObject.parseObject(value).getJSONObject("data");
                    String table = JSONObject.parseObject(value).getString("table");
                    TableProcess tableProcess = readTpBcState.get(table);

                    if (tableProcess != null) {
                        tableProcess.setOperate_type(JSONObject.parseObject(value).getString("type"));
                        out.collect(Tuple2.of(data,tableProcess));
                    }

                }


                @Override
                public void processBroadcastElement(TableProcess tp, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    BroadcastState<String, TableProcess> tpBcState = ctx.getBroadcastState(bcMapStateDesc);
                    tpBcState.put(tp.getSourceTable(), tp);
                    //checkToPhoenix: create tp table to phoenix
                    checkToPhoenix(tp);

                }

                private void checkToPhoenix(TableProcess tp) throws SQLException {
                    //create table if not exists sinkTable (f1 varchar, f2 varchar constraint primary key ("id")) bucket
                    StringBuilder sql = new StringBuilder("create table if not exists ");
                    sql.append(tp.getSinkTable()).append(" ( ")
                        .append(tp.getSinkColumns().replaceAll("([^,]+)","$1 varchar"))
                        .append(", constraint pk primary key(")
                        .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk()).append(")")
                        .append(" ) ")
                        .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
                    System.out.println("sql+++"+sql);
                    PreparedStatement ps = phoenixConn.prepareStatement(sql.toString());
                    ps.execute();
                    ps.close();

                }
            });
    }

    private SingleOutputStreamOperator<TableProcess> tpSourceFromFlinkCDC(StreamExecutionEnvironment env) {
        DataStreamSource<String> tpStream = env.fromSource(FlinkSourceUtil.getFlinkCDCSource(), 
            WatermarkStrategy.noWatermarks(), "FlinkCDC Source");
        
        return tpStream.map(new MapFunction<String, TableProcess>() {
            @Override
            public TableProcess map(String value) throws Exception {
                return JSONObject.parseObject(value).getObject("after", TableProcess.class);
            }
        });
    }

    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> stream) {
        return stream.map(JSONObject::parseObject)
            .filter(new FilterFunction<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) {
                    String type = value.getString("type");
                    try {
                        //data != null and empty && type && database
                        return !Strings.isNullOrEmpty(value.getJSONObject("data").toJSONString())
                            && "gmall".equals(value.getString("database"))
                            && ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type));

                    } catch (Exception e) {
                        System.out.println("Invalid JSON format detected");
                        return false;
                    }

                }
            })
            .map(JSONAware::toJSONString);
    }
}
