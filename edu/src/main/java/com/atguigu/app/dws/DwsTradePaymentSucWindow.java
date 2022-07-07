package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.TradePaymentWindowBean;
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
 * @ClassName DwsTradePaymentSucWindow
 * @Author Chris
 * @Description TODO 统计支付成功独立用户数和首次支付成功用户数
 * @Date 2022/7/7 14:28
 **/

public class DwsTradePaymentSucWindow extends BaseAppV1 {
	public static void main(String[] args) {
		new DwsTradePaymentSucWindow().init(
				2059,
				2,
				"DwsTradePaymentSucWindow",
				Constant.TOPIC_DWD_PAY_SUC_DETAIL
		);
	}

	@Override
	public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
		stream
				.map(JSON::parseObject)
				.keyBy(obj -> obj.getString("user_id"))
				.process(new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {

					private ValueState<String> lastPaymentSucDateState;

					@Override
					public void open(Configuration parameters) throws Exception {
						lastPaymentSucDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastPaymentSucDateState", String.class));
					}

					@Override
					public void processElement(JSONObject obj,
											   Context ctx,
											   Collector<TradePaymentWindowBean> out) throws Exception {
						Long ts = obj.getLong("ts");
						String today = DateFormatUtil.toDate(ts);
						String lastPaymentSucDate = lastPaymentSucDateState.value();
						long paymentSucUniqueUserCount = 0;
						long paymentSucNewUserCount = 0;

						if (!today.equals(lastPaymentSucDate)) {
							paymentSucUniqueUserCount = 1;
							lastPaymentSucDateState.update(today);
							if (lastPaymentSucDate == null) {
								paymentSucNewUserCount = 1;
							}
						}

						if (paymentSucUniqueUserCount == 1) {
							out.collect(new TradePaymentWindowBean("", "", paymentSucUniqueUserCount, paymentSucNewUserCount, ts));
						}
					}
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
								.withTimestampAssigner((bean, ts) -> bean.getTs())
				)
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(new ReduceFunction<TradePaymentWindowBean>() {
							@Override
							public TradePaymentWindowBean reduce(TradePaymentWindowBean value1,
																 TradePaymentWindowBean value2) throws Exception {
								value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
								value2.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
								return value1;
							}
						},
						new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
							@Override
							public void apply(TimeWindow window,
											  Iterable<TradePaymentWindowBean> values,
											  Collector<TradePaymentWindowBean> out) throws Exception {
								TradePaymentWindowBean bean = values.iterator().next();
								bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
								bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
								bean.setTs(System.currentTimeMillis());
								out.collect(bean);
							}
						}
				)
				.addSink(FlinkSinkUtil.getClickHoseSink("dws_trade_payment_suc_window",TradePaymentWindowBean.class));

	}
}
