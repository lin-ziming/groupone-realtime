package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.TestQuestionAnswerBean;
import com.atguigu.common.Constant;
import com.atguigu.util.AtguiguUtil;
import com.atguigu.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
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

public class DwsTestQuestionAnswerWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new DwsTestQuestionAnswerWindow().init(3056, 2, "DwsQuestionAnswerWindow", Constant.TOPIC_DWD_TEST_PAPER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        SingleOutputStreamOperator<JSONObject> distinctStream = distinctByTestScoreDetail(stream);

        SingleOutputStreamOperator<TestQuestionAnswerBean> beanStream = parseToPOJO(distinctStream);

        SingleOutputStreamOperator<TestQuestionAnswerBean> aggregateStream = windowAndAggregate(beanStream);
        aggregateStream.print();

    }


    private SingleOutputStreamOperator<TestQuestionAnswerBean> windowAndAggregate(SingleOutputStreamOperator<TestQuestionAnswerBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TestQuestionAnswerBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((bean, ts) -> bean.getTs()))
                .keyBy(TestQuestionAnswerBean::getQuestionId)
                .window(TumblingEventTimeWindows.of(Time.hours(2)))
                .reduce(new ReduceFunction<TestQuestionAnswerBean>() {

                    @Override
                    public TestQuestionAnswerBean reduce(TestQuestionAnswerBean value1, TestQuestionAnswerBean value2) throws Exception {
                        value1.setCorrectCt(value1.getCorrectCt() + value2.getCorrectCt());
                        value1.setAnswerCt(value1.getAnswerCt() + value2.getAnswerCt());
                        return value1;
                    }
                }, new WindowFunction<TestQuestionAnswerBean, TestQuestionAnswerBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TestQuestionAnswerBean> input, Collector<TestQuestionAnswerBean> out) throws Exception {
                        TestQuestionAnswerBean bean = input.iterator().next();

                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        bean.setCorrectPercent((double) bean.getCorrectCt() / bean.getAnswerCt());

                        out.collect(bean);
                    }
                });

    }

    private SingleOutputStreamOperator<TestQuestionAnswerBean> parseToPOJO(SingleOutputStreamOperator<JSONObject> distinctStream) {
//        return distinctStream.map(new RichMapFunction<JSONObject, QuestionAnswerBean>() {
        return distinctStream.map(new MapFunction<JSONObject, TestQuestionAnswerBean>() {

//            private ValueState<Integer> correctState;
//            private ValueState<String> answerQuestionState;
//
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                answerQuestionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("answerQuestionState", String.class));
//                correctState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("correctState", Integer.class));
//            }

            @Override
            public TestQuestionAnswerBean map(JSONObject value) throws Exception {
                String userId = value.getString("user_id");
                String questionId = value.getString("question_id");
                String isCorrect = value.getString("is_correct");
                int correctCt = 0;
                int answerCvCt = 0;
                int correctCvCt = 0;

                if ("1".equals(isCorrect)) {
                    correctCt = 1;
                } else {
                    correctCt = 0;
                }


//                if (answerQuestionState == null) {
//
//                    if ("1".equals(isCorrect)) {
//                        correctCvCt = 1;
//                    } else {
//                        correctCvCt = 0;
//                    }
//                    answerQuestionState.update(questionId + ":" + userId);
//                    correctState.update(correctCvCt);
//
//                } else {
//
//                    if (answerQuestionState.equals(questionId+ ":" + userId) && "1".equals(correctState.value())){
//
//                    }
//                }

                return new TestQuestionAnswerBean("", "", questionId, correctCt, 1, 0d, value.getLong("ts") * 1000);
            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> distinctByTestScoreDetail(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("exam_id"))
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
