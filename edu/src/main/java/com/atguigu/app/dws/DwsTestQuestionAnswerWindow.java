package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.TestQuestionAnswerBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTestQuestionAnswerWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new DwsTestQuestionAnswerWindow().init(3065, 2, "DwsQuestionAnswerWindow", Constant.TOPIC_DWD_TEST_PAPER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        SingleOutputStreamOperator<TestQuestionAnswerBean> beanStream = parseToPOJO(stream);

        SingleOutputStreamOperator<TestQuestionAnswerBean> aggregateStream = windowAndAggregate(beanStream);
        aggregateStream.print();

        writeToClickhouse(aggregateStream);


    }

    private void writeToClickhouse(SingleOutputStreamOperator<TestQuestionAnswerBean> aggregateStream) {
        aggregateStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_test_question_answer_window",TestQuestionAnswerBean.class));
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
                        value1.setCorrectUvCt(value1.getCorrectUvCt() + value2.getCorrectUvCt());
                        value1.setAnswerUvCt(value1.getAnswerUvCt() + value2.getAnswerUvCt());

                        return value1;
                    }
                }, new WindowFunction<TestQuestionAnswerBean, TestQuestionAnswerBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TestQuestionAnswerBean> input, Collector<TestQuestionAnswerBean> out) throws Exception {
                        TestQuestionAnswerBean bean = input.iterator().next();

                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        bean.setCorrectPercent((double) bean.getCorrectCt() / bean.getAnswerCt());
                        if (bean.getAnswerUvCt() == 0){
                            bean.setCorrectUvPercent(0.0);
                        }else {
                            bean.setCorrectUvPercent((double) bean.getCorrectUvCt() / bean.getAnswerUvCt());
                        }

                        out.collect(bean);
                    }
                });

    }

    private SingleOutputStreamOperator<TestQuestionAnswerBean> parseToPOJO(SingleOutputStreamOperator<String> distinctStream) {
        return distinctStream
                .map(JSONObject::parseObject)
                .keyBy(obj -> obj.getString("user_id") + " " + obj.getString("question_id"))
                .map(new RichMapFunction<JSONObject, TestQuestionAnswerBean>() {

                    private ValueState<Integer> correctState;
                    private ValueState<String> answerQuestionState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        answerQuestionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("answerQuestionState", String.class));
                        correctState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("correctState", Integer.class));
                    }

                    @Override
                    public TestQuestionAnswerBean map(JSONObject value) throws Exception {
                        String examId = value.getString("exam_id");
                        String questionId = value.getString("question_id");
                        String isCorrect = value.getString("is_correct");

//                        System.out.println("  状态  " + answerQuestionState.value());

                        int correctCt = 0;
                        int correctCvCt = 0;
                        int answerUvCt = 0;

                        if ("1".equals(isCorrect)) {
                            correctCt = 1;
                        } else {
                            correctCt = 0;
                        }

                        if (answerQuestionState.value() == null) {
                            answerUvCt = 1;
                            correctCvCt = correctCt;
                            answerQuestionState.update(questionId + ":" +  value.getString("user_id"));

                        } else {
                            answerUvCt = 0;
                            if (correctCvCt == 0 && "1".equals(isCorrect)) {
                                correctCvCt = 1;
                            }
                        }


                        return new TestQuestionAnswerBean("", "", questionId, correctCt, 1, 0d, correctCvCt, answerUvCt, 0d, value.getLong("ts") * 1000);

                    }
                });
    }

}
