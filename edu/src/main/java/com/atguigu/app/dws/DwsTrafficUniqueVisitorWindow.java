package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV3;
import com.atguigu.bean.TradeSourceProvinceOrder;
import com.atguigu.bean.TrafficUniqueVisitor;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
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
public class DwsTrafficUniqueVisitorWindow extends BaseAppV3 {
    private static final String APPNAME = "DwsTrafficUniqueVisitorWindow";

    public static void main(String[] args) {
        Map<String, Long[]> map = new HashMap<>();
        map.put(Constant.TOPIC_DWD_TRAFFIC_UV, new Long[]{});
        new DwsTrafficUniqueVisitorWindow().init(
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
                .map(new MapFunction<String, TrafficUniqueVisitor>() {
                    @Override
                    public TrafficUniqueVisitor map(String json) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(json);
                        String userId = jsonObject.getJSONObject("common").getString("uid");
                        Long ts = jsonObject.getLong("ts");
                        return new TrafficUniqueVisitor("", "", 0L, ts, new HashSet<>(Collections.singleton(userId)));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficUniqueVisitor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<TrafficUniqueVisitor>() {
                            @Override
                            public TrafficUniqueVisitor reduce(TrafficUniqueVisitor v1, TrafficUniqueVisitor v2) throws Exception {
                                v1.getUserIdSet().addAll(v2.getUserIdSet());
                                return v1;
                            }
                        },
                        new AllWindowFunction<TrafficUniqueVisitor, TrafficUniqueVisitor, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<TrafficUniqueVisitor> values, Collector<TrafficUniqueVisitor> out) throws Exception {
                                TrafficUniqueVisitor value = values.iterator().next();
                                value.setCount((long) value.getUserIdSet().size());
                                value.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                                value.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                                value.setTs(System.currentTimeMillis());
                                out.collect(value);
                            }
                        }
                )
                .addSink(
                        FlinkSinkUtil.getClickHoseSink(Constant.DWS_TRAFFIC_UV_WINDOW, TrafficUniqueVisitor.class)
                );
    }
}