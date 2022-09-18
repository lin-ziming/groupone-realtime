package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV3;
import com.atguigu.bean.BaseSource;
import com.atguigu.bean.TrafficSourceUV;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.Types.TUPLE;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/6 20:41
 */
public class DwsTrafficSourceUVWindow extends BaseAppV3 {
    private static final String APPNAME = "DwsTrafficSourceUVWindow";
    private static MapStateDescriptor<String, BaseSource> baseSourceState;

    public static void main(String[] args) {
        Map<String, Long[]> map = new HashMap<>();
        map.put(Constant.TOPIC_DWD_TRAFFIC_UV, new Long[]{});
        new DwsTrafficSourceUVWindow().init(
                12001,
                2,
                APPNAME,
                map
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> streams) {

        // 将base_source表做成广播流
        BroadcastStream<BaseSource> sourceBcStream = broadcastBaseSource(env);

        // 从dwd_traffic_uv表获取必要信息
        SingleOutputStreamOperator<TrafficSourceUV> uvStream = getUvInfo(streams);
//        uvStream.print();

        // 双流connect,将数据流中的source更改为文字
        SingleOutputStreamOperator<TrafficSourceUV> result = connect(sourceBcStream, uvStream);
//        result.print();

        result.addSink(
                FlinkSinkUtil.getClickHoseSink(Constant.DWS_TRAFFIC_SOURCE_UV_WINDOW, TrafficSourceUV.class)
        );
    }

    private SingleOutputStreamOperator<TrafficSourceUV> connect(BroadcastStream<BaseSource> bcStream, SingleOutputStreamOperator<TrafficSourceUV> dataStream) {
        return dataStream
                .connect(bcStream)
                .process(new bcProcessFunction());
    }

    private SingleOutputStreamOperator<TrafficSourceUV> getUvInfo(Map<String, DataStreamSource<String>> streams) {
        return streams
                .get(Constant.TOPIC_DWD_TRAFFIC_UV)
                .map(new MapFunction<String, TrafficSourceUV>() {
                    @Override
                    public TrafficSourceUV map(String json) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(json);
                        String userId = jsonObject.getJSONObject("common").getString("uid");
                        String source = jsonObject.getJSONObject("common").getString("sc");
                        Long ts = jsonObject.getLong("ts");
                        return new TrafficSourceUV("", "", source, 0L, ts, new HashSet<>(Collections.singleton(userId)));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficSourceUV>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .keyBy(TrafficSourceUV::getSource)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<TrafficSourceUV>() {
                            @Override
                            public TrafficSourceUV reduce(TrafficSourceUV v1, TrafficSourceUV v2) throws Exception {
                                v1.getUserIdSet().addAll(v2.getUserIdSet());
                                return null;
                            }
                        },
                        new ProcessWindowFunction<TrafficSourceUV, TrafficSourceUV, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<TrafficSourceUV> elements, Collector<TrafficSourceUV> out) throws Exception {
                                TrafficSourceUV value = elements.iterator().next();
                                value.setCount((long) value.getUserIdSet().size());
                                value.setStt(DateFormatUtil.toYmdHms(context.window().getStart()));
                                value.setEdt(DateFormatUtil.toYmdHms(context.window().getEnd()));
                                value.setTs(System.currentTimeMillis());
                                out.collect(value);
                            }
                        }
                );
    }

    private BroadcastStream<BaseSource> broadcastBaseSource(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource
                .<String>builder()
                .hostname("hadoop302")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall")
                .tableList("gmall.base_source")
                .scanNewlyAddedTableEnabled(false)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        SingleOutputStreamOperator<BaseSource> sourceStream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "flink-cdc-baseSource")
                .map(new MapFunction<String, BaseSource>() {
                    @Override
                    public BaseSource map(String value) throws Exception {
                        // cdc同步格式是before+after的json字符串
                        JSONObject baseSource = JSON.parseObject(value).getJSONObject("after");
                        return new BaseSource(baseSource.getString("id"), baseSource.getString("source_site"));
                    }
                });
        baseSourceState = new MapStateDescriptor<>("BaseSourceState", String.class, BaseSource.class);
        return sourceStream.broadcast(baseSourceState);
    }

    // 静态内部类,实现广播processFunction具体方法,实现序列化
    // 匿名内部类function的类型implements Serializable,失败,必需使用非内部类或静态内部类
    static class bcProcessFunction extends BroadcastProcessFunction<TrafficSourceUV, BaseSource, TrafficSourceUV> implements Serializable {
        @Override
        public void processElement(TrafficSourceUV value, ReadOnlyContext ctx, Collector<TrafficSourceUV> out) throws Exception {
            ReadOnlyBroadcastState<String, BaseSource> broadcastState = ctx.getBroadcastState(baseSourceState);
            BaseSource baseSource = broadcastState.get(value.getSource());
            if (baseSource != null) {
                value.setSource(baseSource.getSourceName());
                out.collect(value);
            }
        }

        @Override
        public void processBroadcastElement(BaseSource value, Context ctx, Collector<TrafficSourceUV> out) throws Exception {
            BroadcastState<String, BaseSource> broadcastState = ctx.getBroadcastState(baseSourceState);
            broadcastState.put(value.getSourceId(), value);
        }
    }
}