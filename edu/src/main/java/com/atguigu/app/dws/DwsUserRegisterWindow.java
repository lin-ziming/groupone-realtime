package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.UserRegisterBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName DwsUserRegisterWindow
 * @Author Chris
 * @Description Dws层用户注册汇总表
 * @Date 2022/7/6 8:35
 **/

public class DwsUserRegisterWindow extends BaseAppV1 {
	public static void main(String[] args) {
		new DwsUserRegisterWindow().init(
				2003,
				2,
				"DwsUserRegisterWindow",
				Constant.TOPIC_DWD_USER_REGISTER
		);
	}

	@Override
	public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
		stream
				.map(json -> new UserRegisterBean(
						"",
						"",
						1L,
						JSON.parseObject(json).getLong("ts") * 1000
				))
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
								.withTimestampAssigner((userBean, ts) -> userBean.getTs()))
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(
						new ReduceFunction<UserRegisterBean>() {
							@Override
							public UserRegisterBean reduce(UserRegisterBean value1,
														   UserRegisterBean value2) throws Exception {
								value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
								return value1;
							}

						},
						new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
							@Override
							public void apply(TimeWindow window,
											  Iterable<UserRegisterBean> values,
											  Collector<UserRegisterBean> out) throws Exception {
								UserRegisterBean bean = values.iterator().next();
								bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
								bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
								bean.setTs(System.currentTimeMillis());

								out.collect(bean);
							}
						}
						)
				;// TODO: 写入clickHouse

	}
}
