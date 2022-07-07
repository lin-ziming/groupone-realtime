package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.InteractionReviewBean;
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

public class DwsInteractionReviewWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new DwsInteractionReviewWindow().init(
            3009,
            2,
            "DwsInteractionReviewWindow",
            Constant.TOPIC_DWD_INTERACTION_REVIEW
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<InteractionReviewBean> beanStream = parsetToPojo(stream.map(JSON::parseObject));
        SingleOutputStreamOperator<InteractionReviewBean> streamWithoutDim = windowAndAggregate(beanStream);
        SingleOutputStreamOperator<InteractionReviewBean> streamWithDim = joinDim(streamWithoutDim);
        writeToClickHouse(streamWithDim);
    }

    private void writeToClickHouse(SingleOutputStreamOperator<InteractionReviewBean> stream) {
        stream.addSink(FlinkSinkUtil.getClickHoseSink("dws_interaction_review_window", InteractionReviewBean.class));
    }

    private SingleOutputStreamOperator<InteractionReviewBean> joinDim(SingleOutputStreamOperator<InteractionReviewBean> stream) {
        return stream
            .map(new RichMapFunction<InteractionReviewBean, InteractionReviewBean>() {
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
                public InteractionReviewBean map(InteractionReviewBean bean) throws Exception {
                    JSONObject courseInfo = DimUtil2.readDimFromPhoenix(phoenixConn, "dim_course_info", bean.getCourseId());
                    bean.setCourseName(courseInfo.getString("COURSE_NAME"));
                    return bean;
                }
            });
        
    }
    
    private SingleOutputStreamOperator<InteractionReviewBean> windowAndAggregate(
        SingleOutputStreamOperator<InteractionReviewBean> stream) {
        return stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<InteractionReviewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(InteractionReviewBean::getCourseId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<InteractionReviewBean>() {
                    @Override
                    public InteractionReviewBean reduce(InteractionReviewBean value1,
                                                       InteractionReviewBean value2) throws Exception {
                        return value1;
                    }
                },
                new ProcessWindowFunction<InteractionReviewBean, InteractionReviewBean, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<InteractionReviewBean> elements,
                                        Collector<InteractionReviewBean> out) throws Exception {
                        InteractionReviewBean bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<InteractionReviewBean> parsetToPojo(SingleOutputStreamOperator<JSONObject> stream) {
        return stream.map(new MapFunction<JSONObject, InteractionReviewBean>() {
            @Override
            public InteractionReviewBean map(JSONObject value) throws Exception {
                InteractionReviewBean bean = InteractionReviewBean.builder()
                        .userId(value.getString("user_id"))
                        .courseId(value.getString("course_id"))
                        .reviewStars(value.getString("review_stars"))
                        .ts(value.getLong("ts") * 1000)
                        .build();

                return bean;
            }
        });
    }
}

