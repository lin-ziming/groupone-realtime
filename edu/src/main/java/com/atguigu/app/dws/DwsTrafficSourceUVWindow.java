package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV3;
import com.atguigu.bean.TrafficSourceUV;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/6 20:41
 */
public class DwsTrafficSourceUVWindow extends BaseAppV3 {
    private static final String APPNAME = "DwsTrafficSourceUVWindow";

    public static void main(String[] args) {
        Map<String, Long[]> map = new HashMap<>();
        map.put(Constant.TOPIC_DWD_TRAFFIC_UV, new Long[]{0L, 0L});
        new DwsTrafficSourceUVWindow().init(
                12001,
                2,
                APPNAME,
                map
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> streams) {
        streams
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
                )
                .addSink(
                        FlinkSinkUtil.getClickHoseSink(Constant.DWS_TRAFFIC_SOURCE_UV_WINDOW, TrafficSourceUV.class)
                );
    }
}