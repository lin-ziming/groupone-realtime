package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.CartAddUuBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName DwsTradeCartAddUuWindow
 * @Author Chris
 * @Description TODO
 * @Date 2022/7/7 9:45
 **/

public class DwsTradeCartAddUuWindow extends BaseAppV1 {
	public static void main(String[] args) {
		new DwsTradeCartAddUuWindow().init(
				2010,
				2,
				"DwsTradeCartAddUuWindow",
				Constant.TOPIC_DWD_TRADE_CART_ADD
		);
	}

	@Override
	public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

		stream
				.map(JSON::parseObject)
				.keyBy(obj -> obj.getString("user_id"))
				.process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {

					private ValueState<String> lastCartAddDateState;

					@Override
					public void open(Configuration parameters) throws Exception {
						lastCartAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastCartAddDateState", String.class));
					}

					@Override
					public void processElement(JSONObject obj,
											   Context ctx,
											   Collector<CartAddUuBean> out) throws Exception {
						Long ts = obj.getLong("ts");
						String today = DateFormatUtil.toDate(ts);
						System.out.println(today);
						if (!today.equals(lastCartAddDateState.value())) {
							CartAddUuBean bean = new CartAddUuBean("", "", 1L, ts);
							out.collect(bean);
							lastCartAddDateState.update(today);
						}

					}
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
								.withTimestampAssigner((bean, ts) -> bean.getTs())
				)
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(new ReduceFunction<CartAddUuBean>() {
							@Override
							public CartAddUuBean reduce(CartAddUuBean value1,
														CartAddUuBean value2) throws Exception {
								value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
								return value1;
							}
						},
						new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
							@Override
							public void apply(TimeWindow window,
											  Iterable<CartAddUuBean> values,
											  Collector<CartAddUuBean> out) throws Exception {
								CartAddUuBean bean = values.iterator().next();
								bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
								bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
								bean.setTs(System.currentTimeMillis());
								out.collect(bean);
							}
						}
				)
				.addSink(FlinkSinkUtil.getClickHoseSink("dws_trade_cart_add_uu_window", CartAddUuBean.class));

	}
}
