package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.UserActiveAndBackBean;
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
 * @ClassName DwsUserUserLoginWindow
 * @Author Chris
 * @Description TODO Dws活跃用户和回流用户汇总表
 * @Date 2022/7/6 18:59
 **/

public class DwsUserUserLoginWindow extends BaseAppV1 {
	public static void main(String[] args) {
		new DwsUserUserLoginWindow().init(
				2008,
				2,
				"DwsUserUserLoginWindow",
				Constant.TOPIC_DWD_TRAFFIC_UV
		);
	}

	@Override
	public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
		SingleOutputStreamOperator<UserActiveAndBackBean> beanStream = findActiveAndBack(stream);

		SingleOutputStreamOperator<UserActiveAndBackBean> resultStream = windowAndReduce(beanStream);

		writeToclickHouse(resultStream);
	}

	private void writeToclickHouse(SingleOutputStreamOperator<UserActiveAndBackBean> resultStream) {
		resultStream.addSink(FlinkSinkUtil.getClickHoseSink(
				"dws_user_user_active_and_back_window",
				UserActiveAndBackBean.class));
	}

	private SingleOutputStreamOperator<UserActiveAndBackBean> windowAndReduce(SingleOutputStreamOperator<UserActiveAndBackBean> beanStream) {
		return beanStream
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<UserActiveAndBackBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
								.withTimestampAssigner((bean, ts) -> bean.getTs())
				)
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
				.reduce(new ReduceFunction<UserActiveAndBackBean>() {
							@Override
							public UserActiveAndBackBean reduce(UserActiveAndBackBean value1,
																UserActiveAndBackBean value2) throws Exception {
								value1.setBackCt(value1.getBackCt() + value2.getBackCt());
								value1.setActiveCt(value1.getActiveCt() + value2.getActiveCt());
								return value1;
							}
						},
						new AllWindowFunction<UserActiveAndBackBean, UserActiveAndBackBean, TimeWindow>() {
							@Override
							public void apply(TimeWindow window,
											  Iterable<UserActiveAndBackBean> values,
											  Collector<UserActiveAndBackBean> out) throws Exception {
								UserActiveAndBackBean reducedBean = values.iterator().next();
								reducedBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
								reducedBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

								reducedBean.setTs(System.currentTimeMillis());
								out.collect(reducedBean);
							}
						}
				);
	}

	private SingleOutputStreamOperator<UserActiveAndBackBean> findActiveAndBack(DataStreamSource<String> stream) {

		return stream
				.map(JSON::parseObject)
				.keyBy(obj -> obj.getJSONObject("common").getString("uid"))
				.process(new KeyedProcessFunction<String, JSONObject, UserActiveAndBackBean>() {

					private ValueState<String> lastLoginDateState;

					@Override
					public void open(Configuration parameters) throws Exception {
						lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDateState", String.class));
					}

					@Override
					public void processElement(JSONObject obj,
											   Context ctx,
											   Collector<UserActiveAndBackBean> out) throws Exception {
						Long ts = obj.getLong("ts");
						String today = DateFormatUtil.toDate(ts);
						String lastLoginDate = lastLoginDateState.value();

						long backCt = 0;
						long activeCt = 1;
						lastLoginDateState.update(today);
						System.out.println("uid: " + ctx.getCurrentKey());

						if (lastLoginDate != null) {
							Long lastLoginTs = DateFormatUtil.toTs(lastLoginDate);
							if ((ts - lastLoginTs) / 1000 / 60 / 60 / 24 > 3) {
								backCt = 1;
							}
						}
						UserActiveAndBackBean bean = new UserActiveAndBackBean("", "", backCt, activeCt, ts);
						System.out.println(bean);
						out.collect(bean);
					}
				});
	}
}
