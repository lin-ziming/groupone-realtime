package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.TestCourseExamBean;
import com.atguigu.common.Constant;
import com.atguigu.util.AtguiguUtil;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
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

public class DwsTestCourseExamWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new DwsTestCourseExamWindow().init(3507, 2, "DwsCourseExamWindow", Constant.TOPIC_DWD_TEST_SCORE_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        SingleOutputStreamOperator<JSONObject> distinctStream = distinctByTestScoreDetail(stream);

        SingleOutputStreamOperator<TestCourseExamBean> beanStream = parseToPOJO(distinctStream);

        SingleOutputStreamOperator<TestCourseExamBean> aggregateStream = windowAndAggregate(beanStream);
        aggregateStream.print();

        writeToClickhouse(aggregateStream);

    }

    private void writeToClickhouse(SingleOutputStreamOperator<TestCourseExamBean> aggregateStream) {
        aggregateStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_test_course_exam_window",TestCourseExamBean.class));
    }

    private SingleOutputStreamOperator<TestCourseExamBean> windowAndAggregate(SingleOutputStreamOperator<TestCourseExamBean> beanStream) {
        return beanStream.
                assignTimestampsAndWatermarks(WatermarkStrategy.<TestCourseExamBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((bean, ts) -> bean.getTs()))
                .keyBy(bean -> bean.getCourseId())
                .window(TumblingEventTimeWindows.of(Time.hours(2)))
                .reduce(new ReduceFunction<TestCourseExamBean>() {
                    @Override
                    public TestCourseExamBean reduce(TestCourseExamBean value1, TestCourseExamBean value2) throws Exception {
                        value1.setDurationSec(value1.getDurationSec() + value2.getDurationSec());
                        value1.setScore(value1.getScore() + value2.getScore());
                        value1.getUserIdSet().addAll(value2.getUserIdSet());
                        return value1;
                    }
                }, new WindowFunction<TestCourseExamBean, TestCourseExamBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TestCourseExamBean> input, Collector<TestCourseExamBean> out) throws Exception {
                        TestCourseExamBean bean = input.iterator().next();

                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        bean.setExamNum(bean.getUserIdSet().size());

                        bean.setAvgDuringSec((double) bean.getDurationSec() / bean.getExamNum());
                        bean.setAvgScore(bean.getScore() / bean.getExamNum());

                        bean.setTs(System.currentTimeMillis());

                        out.collect(bean);
                    }
                });


    }

    private SingleOutputStreamOperator<TestCourseExamBean> parseToPOJO(SingleOutputStreamOperator<JSONObject> distinctStream) {
        return distinctStream.map(new MapFunction<JSONObject, TestCourseExamBean>() {
            @Override
            public TestCourseExamBean map(JSONObject value) throws Exception {
                return TestCourseExamBean.builder()
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
                        // ????????????????????????, ??????????????????????????????????????????????????????: ??????????????????????????????
                        out.collect(maxDateDataState.value());
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        if (maxDateDataState.value() == null) {
                            // ?????????????????????
                            // 1. ???????????????: 5s?????????????????????
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                            // 2.????????????
                            maxDateDataState.update(value);

                        } else {
                            // ???????????????
                            // 3. ????????????, ??????????????????????????????, ??????????????????????????????(????????????)
                            // "2022-06-27 01:04:48.839Z"   "2022-06-27 01:04:48.9z"
                            String current = value.getString("pt");
                            String last = maxDateDataState.value().getString("pt");
                            // ??????current >= last ???????????????
                            boolean isGreaterOrEqual = AtguiguUtil.compareLTZ(current, last);  // ??????current >= last ?????????true, ????????????false
                            if (isGreaterOrEqual) {
                                maxDateDataState.update(value);
                            }

                        }
                    }
                });
    }


}
