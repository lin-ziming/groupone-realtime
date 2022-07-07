package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.TradeCourseOrderBean;
import com.atguigu.common.Constant;
import com.atguigu.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Connection;
import java.time.Duration;

public class DwsTradeCourseOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new DwsTradeCourseOrderWindow().init(
            3009,
            2,
            "DwsTradeCourseOrderWindow",
            Constant.TOPIC_DWD_ORDER_DETAIL
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // order_detail_id进行去重
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);
        // 封装到pojo中
        SingleOutputStreamOperator<TradeCourseOrderBean> beanStream = parsetToPojo(distinctedStream);
        // 按照course_id进行分组开窗聚合
        SingleOutputStreamOperator<TradeCourseOrderBean> streamWithoutDim = windowAndAggregate(beanStream);
        // 补充维度信息
        SingleOutputStreamOperator<TradeCourseOrderBean> streamWithDim = joinDim(streamWithoutDim);
        // 写出到clickhouse
        writeToClickHouse(streamWithDim);

    }

    private void writeToClickHouse(SingleOutputStreamOperator<TradeCourseOrderBean> stream) {
        stream.addSink(FlinkSinkUtil.getClickHoseSink("dws_trade_course_order_window", TradeCourseOrderBean.class));
    }

    private SingleOutputStreamOperator<TradeCourseOrderBean> joinDim(SingleOutputStreamOperator<TradeCourseOrderBean> stream) {
        return stream
            .map(new RichMapFunction<TradeCourseOrderBean, TradeCourseOrderBean>() {
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
                public TradeCourseOrderBean map(TradeCourseOrderBean bean) throws Exception {
                    JSONObject courseInfo = DimUtil2.readDimFromPhoenix(phoenixConn, "dim_course_info", bean.getCourseId());
                    bean.setCourseName(courseInfo.getString("COURSE_NAME"));
                    bean.setSubjectId(courseInfo.getString("SUBJECT_ID"));

                    JSONObject baseSubjectInfo = DimUtil2.readDimFromPhoenix(phoenixConn, "dim_base_subject_info", bean.getSubjectId());
                    bean.setSubjectName(baseSubjectInfo.getString("SUBJECT_NAME"));
                    bean.setCategoryId(baseSubjectInfo.getString("CATEGORY_ID"));

                    JSONObject baseCategoryInfo = DimUtil2.readDimFromPhoenix(phoenixConn, "dim_base_category_info", bean.getCategoryId());
                    bean.setCategoryName(baseCategoryInfo.getString("CATEGORY_NAME"));

                    return bean;
                }
            });
        
    }
    
    private SingleOutputStreamOperator<TradeCourseOrderBean> windowAndAggregate(
        SingleOutputStreamOperator<TradeCourseOrderBean> stream) {
        return stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeCourseOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(TradeCourseOrderBean::getCourseId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeCourseOrderBean>() {
                    @Override
                    public TradeCourseOrderBean reduce(TradeCourseOrderBean value1,
                                                       TradeCourseOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeCourseOrderBean, TradeCourseOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<TradeCourseOrderBean> elements,
                                        Collector<TradeCourseOrderBean> out) throws Exception {
                        
                        TradeCourseOrderBean bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));
                        bean.setTs(System.currentTimeMillis());

                        bean.setOrderCount((long) bean.getOrderIdSet().size());
                        
                        out.collect(bean);

                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TradeCourseOrderBean> parsetToPojo(SingleOutputStreamOperator<JSONObject> stream) {
        return stream.map(new MapFunction<JSONObject, TradeCourseOrderBean>() {
            @Override
            public TradeCourseOrderBean map(JSONObject value) throws Exception {
                TradeCourseOrderBean bean = TradeCourseOrderBean.builder()
                    .userId(value.getString("user_id"))
                    .courseId(value.getString("course_id"))
                    //.courseName(value.getString("course_name"))
                    .orderAmount(value.getDoubleValue("split_final_amount"))
                    .ts(value.getLong("ts"))
                    .build();
                
                bean.getOrderIdSet().add(value.getString("order_id"));

                return bean;
            }
        });
    }

    //根据order_detail_id去重
    private SingleOutputStreamOperator<JSONObject> distinctByOrderDetailId(DataStreamSource<String> stream) {
        return stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getString("order_detail_id"))
            .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                private ValueState<JSONObject> maxDateDataState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    maxDateDataState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("maxDateDataState", JSONObject.class));
                    
                }
                
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                    out.collect(maxDateDataState.value());
                }
                
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    if (maxDateDataState.value() == null) {
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                        maxDateDataState.update(value);
                        
                    } else {
                        String current = value.getString("row_op_ts");
                        String last = maxDateDataState.value().getString("row_op_ts");
                        boolean isGreaterOrEqual = AtguiguUtil.compareLTZ(current, last);
                        if (isGreaterOrEqual) {
                            maxDateDataState.update(value);
                        }
                    }
                }
            });
    }
}
