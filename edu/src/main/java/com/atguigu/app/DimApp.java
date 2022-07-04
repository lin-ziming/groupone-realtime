package com.atguigu.app;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.Constant;
import com.atguigu.util.FlinkSourceUtil;
import com.google.common.base.Strings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import javax.swing.plaf.IconUIResource;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/4 15:07
 */
public class DimApp extends BaseAppV1{

    public static void main(String[] args) {
        new DimApp().init(11041, 2, "DimApp", Constant.TOPIC_ODS_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<String> etlStream = etl(stream);
//        etlStream.print();
        SingleOutputStreamOperator<TableProcess> tpStream = tpSourceFromFlinkCDC(env);
//        tpStream.print();

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connect(etlStream, tpStream);
        connectedStream.print();
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<String> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        MapStateDescriptor<String, TableProcess> bcMapStateDesc = new MapStateDescriptor<>("broadcastMapState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(bcMapStateDesc);
        return dataStream.connect(bcStream)
            .process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>() {
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
                    //todo: checkToPhoenix
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
                            && Constant.DATABASE_NAME.equals(value.getString("database"))
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
