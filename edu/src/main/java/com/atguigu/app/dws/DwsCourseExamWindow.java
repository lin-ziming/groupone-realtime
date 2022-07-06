package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.CourseExamBean;
import com.atguigu.bean.CourseExamBean;
import com.atguigu.common.Constant;
import com.atguigu.util.AtguiguUtil;
import com.atguigu.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;

public class DwsCourseExamWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new DwsCourseExamWindow().init(3505, 2, "DwsCourseExamWindow", Constant.TOPIC_DWD_TEST_SCORE_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        SingleOutputStreamOperator<JSONObject> distinctStream = distinctByTestScoreDetail(stream);

        SingleOutputStreamOperator<CourseExamBean> beanStream = parseToPOJO(distinctStream);

        SingleOutputStreamOperator<CourseExamBean> aggregateStream = windowAndAggregate(beanStream);
        aggregateStream.print();

//        writeToClickhouse(aggregateStream);

    }

    private SingleOutputStreamOperator<CourseExamBean> windowAndAggregate(SingleOutputStreamOperator<CourseExamBean> beanStream) {
        return beanStream.
                assignTimestampsAndWatermarks(WatermarkStrategy.<CourseExamBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((bean, ts) -> bean.getTs()))
                .keyBy(bean -> bean.getCourseId())
                .window(TumblingEventTimeWindows.of(Time.hours(2)))
                .reduce(new ReduceFunction<CourseExamBean>() {
                    @Override
                    public CourseExamBean reduce(CourseExamBean value1, CourseExamBean value2) throws Exception {
                        value1.setDurationSec(value1.getDurationSec() + value2.getDurationSec());
                        value1.setScore(value1.getScore() + value2.getScore());
                        value1.getUserIdSet().addAll(value2.getUserIdSet());
                        return value1;
                    }
                }, new WindowFunction<CourseExamBean, CourseExamBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<CourseExamBean> input, Collector<CourseExamBean> out) throws Exception {
                        CourseExamBean bean = input.iterator().next();

                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        bean.setExamNum(bean.getUserIdSet().size());

                        bean.setAvgDuringTime((double) bean.getDurationSec() / bean.getExamNum());
                        bean.setAvgScore(bean.getScore() / bean.getExamNum());

                        bean.setTs(System.currentTimeMillis());

                        out.collect(bean);
                    }
                });


    }

    private SingleOutputStreamOperator<CourseExamBean> parseToPOJO(SingleOutputStreamOperator<JSONObject> distinctStream) {
        return distinctStream.map(new MapFunction<JSONObject, CourseExamBean>() {
            @Override
            public CourseExamBean map(JSONObject value) throws Exception {
                return CourseExamBean.builder()
//                                .examId(value.getString("id"))
                        .courseId(value.getString("course_id"))
                        .userIdSet(new HashSet<>(Collections.singleton(value.getString("user_id"))))
                        .durationSec(value.getLong("duration_sec"))
                        .score(value.getDouble("total_score"))
                        .ts(value.getLong("ts") * 1000)
                        .build();
            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> distinctByTestScoreDetail(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> maxDateDataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        maxDateDataState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("maxDateDataState", JSONObject.class));

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 定时器触发的时候, 状态中保存的一定是时间最大的那条数据: 最后一个最完整的数据
                        out.collect(maxDateDataState.value());
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        if (maxDateDataState.value() == null) {
                            // 第一条数据进来
                            // 1. 注册定时器: 5s后触发的定时器
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                            // 2.更新状态
                            maxDateDataState.update(value);

                        } else {
                            // 不是第一条
                            // 3. 比较时间, 如果新来的时间比较大, 则把这条数据保存下来(更新状态)
                            // "2022-06-27 01:04:48.839Z"   "2022-06-27 01:04:48.9z"
                            String current = value.getString("pt");
                            String last = maxDateDataState.value().getString("pt");
                            // 如果current >= last 则更新状态
                            boolean isGreaterOrEqual = AtguiguUtil.compareLTZ(current, last);  // 如果current >= last 则返回true, 否则返回false
                            if (isGreaterOrEqual) {
                                maxDateDataState.update(value);
                            }

                        }
                    }
                });
    }


}
