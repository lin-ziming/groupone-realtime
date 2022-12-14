package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.TestPaperGradeBean;
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

public class DwsTestPaperGradeWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new DwsTestPaperGradeWindow().init(3504, 2, "DwsPaperGradeWindow", Constant.TOPIC_DWD_TEST_SCORE_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        SingleOutputStreamOperator<JSONObject> distinctStream = distinctByTestScoreDetail(stream);

        SingleOutputStreamOperator<TestPaperGradeBean> beanStream = parseToPOJO(distinctStream);

        SingleOutputStreamOperator<TestPaperGradeBean> aggregateStream = windowAndAggregate(beanStream);
        aggregateStream.print();

        writeToClickhouse(aggregateStream);

    }

    private void writeToClickhouse(SingleOutputStreamOperator<TestPaperGradeBean> aggregateStream) {
        aggregateStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_test_paper_grade_window",TestPaperGradeBean.class));
    }


    private SingleOutputStreamOperator<TestPaperGradeBean> windowAndAggregate(SingleOutputStreamOperator<TestPaperGradeBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TestPaperGradeBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((bean, ts) -> bean.getTs()))
                .keyBy(TestPaperGradeBean::getPaperId)
                .window(TumblingEventTimeWindows.of(Time.hours(2)))
                .reduce(new ReduceFunction<TestPaperGradeBean>() {
                    @Override
                    public TestPaperGradeBean reduce(TestPaperGradeBean value1, TestPaperGradeBean value2) throws Exception {

                        value1.setGreatGroup(value1.getGreatGroup() + value2.getGreatGroup());
                        value1.setGoodGroup(value1.getGoodGroup() + value2.getGoodGroup());
                        value1.setMidGroup(value1.getMidGroup() + value2.getMidGroup());
                        value1.setPoorGroup(value1.getPoorGroup() + value2.getPoorGroup());

                        return value1;
                    }
                }, new WindowFunction<TestPaperGradeBean, TestPaperGradeBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TestPaperGradeBean> input, Collector<TestPaperGradeBean> out) throws Exception {
                        TestPaperGradeBean bean = input.iterator().next();

                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        bean.setCt(bean.getGreatGroup() + bean.getGoodGroup() + bean.getMidGroup() + bean.getPoorGroup());

                        bean.setTs(System.currentTimeMillis());

                        out.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TestPaperGradeBean> parseToPOJO(SingleOutputStreamOperator<JSONObject> distinctStream) {
        return distinctStream
                .map(new MapFunction<JSONObject, TestPaperGradeBean>() {
                    private Double score;
                    private long ts;

                    @Override
                    public TestPaperGradeBean map(JSONObject value) throws Exception {
                        String paperId = value.getString("paper_id");
                        score = value.getDouble("total_score");
                        ts = value.getLong("ts") * 1000;

                        Integer poorGroup = 0;
                        Integer midGroup = 0;
                        Integer goodGroup = 0;
                        Integer greatGroup = 0;

                        if (score >= 80) {
                            greatGroup = 1;
                        } else if (score < 80 && score >= 70) {
                            goodGroup = 1;
                        } else if (score < 70 && score >= 60) {
                            midGroup = 1;
                        } else if (score < 60) {
                            poorGroup = 1;
                        }


                        return new TestPaperGradeBean("", "", paperId, greatGroup, goodGroup, midGroup, poorGroup, 0, ts);
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
