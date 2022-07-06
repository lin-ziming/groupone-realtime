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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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
 * @Create_time: 2022/7/6 18:13
 */
public class DwsTradeSourceProvinceOrder extends BaseAppV3 {

    private static final String APPNAME = "DwsTradeSourceProvinceOrder";

    public static void main(String[] args) {
        Map<String, Long[]> map = new HashMap<>();
        map.put(Constant.TOPIC_DWD_ORDER_DETAIL, new Long[]{});
        new DwsTradeSourceProvinceOrder().init(
                12000,
                2,
                APPNAME,
                map
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> streams) {
        streams
                .get(Constant.TOPIC_DWD_ORDER_DETAIL)
                .map(new MapFunction<String, TradeSourceProvinceOrder>() {
                    @Override
                    public TradeSourceProvinceOrder map(String json) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(json);
                        return new TradeSourceProvinceOrder(
                                "", "",
                                jsonObject.getString("source_name"),
                                jsonObject.getString("province_name"),
                                0L, 0L,
                                jsonObject.getDoubleValue("split_final_amount"),
                                jsonObject.getLong("ts"),
                                new HashSet<>(Collections.singleton(jsonObject.getString("order_id"))),
                                new HashSet<>(Collections.singleton(jsonObject.getString("user_id")))
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSourceProvinceOrder>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .keyBy(bean -> bean.getSource() + bean.getProvince())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<TradeSourceProvinceOrder>() {
                            @Override
                            public TradeSourceProvinceOrder reduce(TradeSourceProvinceOrder v1, TradeSourceProvinceOrder v2) throws Exception {
                                v1.setAmount(v1.getAmount() + v2.getAmount());
                                v1.getOrderIdSet().addAll(v2.getOrderIdSet());
                                v1.getUserIdSet().addAll(v2.getUserIdSet());
                                return v1;
                            }
                        }, new ProcessWindowFunction<TradeSourceProvinceOrder, TradeSourceProvinceOrder, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<TradeSourceProvinceOrder> elements, Collector<TradeSourceProvinceOrder> out) throws Exception {
                                TradeSourceProvinceOrder value = elements.iterator().next();
                                value.setStt(DateFormatUtil.toYmdHms(context.window().getStart()));
                                value.setEdt(DateFormatUtil.toYmdHms(context.window().getEnd()));
                                value.setCount((long) value.getUserIdSet().size());
                                value.setTimes((long) value.getOrderIdSet().size());
                                value.setTs(context.currentProcessingTime());
                                out.collect(value);
                            }
                        }
                )
                .addSink(
                        FlinkSinkUtil.getClickHoseSink(Constant.DWS_TRADE_SOURCE_PROVINCE_ORDER_WINDOW, TradeSourceProvinceOrder.class)
                );
    }
}