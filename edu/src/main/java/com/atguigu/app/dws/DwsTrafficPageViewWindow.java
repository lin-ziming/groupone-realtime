package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName DwsTrafficPageViewWindow
 * @Author Chris
 * @Description TODO Dws层页面浏览汇总表
 * @Date 2022/7/6 10:32
 **/

public class DwsTrafficPageViewWindow extends BaseAppV1 {
	public static void main(String[] args) {
		new DwsTrafficPageViewWindow().init(
				2005,
				2,
				"DwsTrafficPageViewWindow",
				Constant.TOPIC_DWD_TRAFFIC_PAGE
		);
	}

	@Override
	public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
		//找到page数据中用户的一条home访问记录和一条course_detail访问记录（如果有的话）
		SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = findUv(stream);
		//开窗聚合，在上一步已经去过重了，所以不用keyBy，windowAll聚合
		SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream = windowAndReduce(beanStream);

		//写出到clickHouse
		writeToClickHouse(resultStream);
	}

	private void writeToClickHouse(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream) {
		resultStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_traffic_page_view_window", TrafficHomeDetailPageViewBean.class));
	}

	private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndReduce(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
		return beanStream
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(
						new ReduceFunction<TrafficHomeDetailPageViewBean>() {
							@Override
							public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1,
																		TrafficHomeDetailPageViewBean bean2) throws Exception {
								bean1.setHomeUvCt(bean1.getHomeUvCt() + bean2.getHomeUvCt());
								bean1.setCourseDetailUvCt(bean1.getCourseDetailUvCt() + bean2.getCourseDetailUvCt());
								return bean1;
							}
						},
						new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
							@Override
							public void apply(TimeWindow window,
											  Iterable<TrafficHomeDetailPageViewBean> values,
											  Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
								TrafficHomeDetailPageViewBean bean = values.iterator().next();
								bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
								bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
								bean.setTs(System.currentTimeMillis());
								out.collect(bean);
							}
						}
				);
	}

	private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> findUv(DataStreamSource<String> stream) {

		return stream
				.map(JSON::parseObject)
				.filter(obj -> {
					String pageId = obj.getJSONObject("page").getString("page_id");
					return "home".equals(pageId) || "course_detail".equals(pageId);
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
								.withTimestampAssigner((obj, ts) -> obj.getLong("ts")))
				.keyBy(obj -> obj.getJSONObject("common").getString("mid"))
				.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

					private ValueState<String> homeState;
					private ValueState<String> courseDetailState;

					@Override
					public void open(Configuration parameters) throws Exception {
						homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("homeState", String.class));
						courseDetailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("courseDetailState", String.class));
					}

					@Override
					public void processElement(JSONObject obj,
											   Context ctx,
											   Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

						String pageId = obj.getJSONObject("page").getString("page_id");
						Long ts = obj.getLong("ts");
						String today = DateFormatUtil.toDate(ts);

						long homeUvCt = 0L;
						long courseDetailUvCt = 0L;

						if ("home".equals(pageId) && !today.equals(homeState.value())) {
							homeUvCt = 1L;
							homeState.update(today);
						} else if ("course_detail".equals(pageId) && !today.equals(courseDetailState.value())) {
							courseDetailUvCt = 1L;
							courseDetailState.update(today);
						}

						if (homeUvCt + courseDetailUvCt == 1) {
							out.collect(new TrafficHomeDetailPageViewBean("", "", homeUvCt, courseDetailUvCt, ts));
						}

					}
				});


	}
}
