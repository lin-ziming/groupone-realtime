package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName BaseLogApp
 * @Author Chris
 * @Description TODO
 * @Date 2022/7/4 14:48
 **/

public class BaseLogApp extends BaseAppV1 {
	private final String PAGE = "page";
	private final String ERR = "err";
	private final String DISPLAY = "display";
	private final String ACTION = "action";
	private final String START = "start";
	private final String APPVIDEO = "appVideo";

	public static void main(String[] args) {
		new BaseLogApp().init(2001,
				2,
				"BaseLogApp",
				Constant.TOPIC_ODS_LOG);
	}

	@Override
	public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
		//1.etl清洗
		SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
		//2.isNew复查
		SingleOutputStreamOperator<JSONObject> checkedStream = isNewCheck(etledStream);
		//3.分流
		Map<String, DataStream<String>> streams = spiltStream(checkedStream);
		//4.写到kafka中
		writeToKafka(streams);

	}
	private void writeToKafka(Map<String, DataStream<String>> streams) {
		streams.get(PAGE).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
		streams.get(START).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
		streams.get(ERR).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
		streams.get(DISPLAY).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
		streams.get(ACTION).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
		streams.get(APPVIDEO).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_APPVIDEO));
	}

	private Map<String, DataStream<String>> spiltStream(SingleOutputStreamOperator<JSONObject> checkedStream) {
		//将log数据进行分流
		//主流start，侧输出流：appVideo page actions displays err
		//一条数据，要么是start，要么事appVideo，要么是其它4种，所以最后拆分出6个topic写进kafka
		OutputTag<String> errTag = new OutputTag<String>(ERR) {};
		OutputTag<String> displayTag = new OutputTag<String>(DISPLAY) {};
		OutputTag<String> actionTag = new OutputTag<String>(ACTION) {};
		OutputTag<String> pageTag = new OutputTag<String>(PAGE) {};
		OutputTag<String> appVideoTag = new OutputTag<String>(APPVIDEO) {};

		SingleOutputStreamOperator<String> startStream = checkedStream
				.process(new ProcessFunction<JSONObject, String>() {
					@Override
					public void processElement(JSONObject obj,
											   Context ctx,
											   Collector<String> out) throws Exception {
						if (obj.containsKey("err")) {
							ctx.output(errTag, obj.toJSONString());

							obj.remove("err");
						}

						if (obj.containsKey("start")) {
							out.collect(obj.toString());
						} else if (obj.containsKey("appVideo")) {
							ctx.output(appVideoTag, obj.toString());
						} else {
							JSONObject common = obj.getJSONObject("common");
							JSONObject page = obj.getJSONObject("page");
							Long ts = obj.getLong("ts");


							JSONArray displays = obj.getJSONArray("displays");
							if (displays != null) {
								for (int i = 0; i < displays.size(); i++) {
									JSONObject display = displays.getJSONObject(i);

									display.putAll(common);
									display.putAll(page);
									display.put("ts", ts);

									ctx.output(displayTag, display.toJSONString());

								}
								obj.remove("displays");
							}


							JSONArray actions = obj.getJSONArray("actions");
							if (actions != null) {
								for (int i = 0; i < actions.size(); i++) {
									JSONObject action = actions.getJSONObject(i);
									action.putAll(common);
									action.putAll(page);

									ctx.output(actionTag, action.toJSONString());
								}
								obj.remove("actions");
							}

							if (obj.containsKey("page")) {
								ctx.output(pageTag, obj.toJSONString());
							}

						}
					}
				});

		Map<String, DataStream<String>> result = new HashMap<>();
		result.put(PAGE, startStream.getSideOutput(pageTag));
		result.put(ERR, startStream.getSideOutput(errTag));
		result.put(DISPLAY, startStream.getSideOutput(displayTag));
		result.put(ACTION, startStream.getSideOutput(actionTag));
		result.put(APPVIDEO, startStream.getSideOutput(appVideoTag));
		result.put(START, startStream);

		return result;

	}

	private SingleOutputStreamOperator<JSONObject> isNewCheck(SingleOutputStreamOperator<JSONObject> etledStream) {
		//将is_new显示不正确的情况进行修正
		return etledStream
				.keyBy(obj -> obj.getJSONObject("common").getString("mid"))
				.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

					private ValueState<String> firstVisitDateState;

					@Override
					public void open(Configuration parameters) throws Exception {
						firstVisitDateState = getRuntimeContext()
								.getState(new ValueStateDescriptor<String>("firstVisitDateState", String.class));
					}

					@Override
					public void processElement(JSONObject obj,
											   Context ctx,
											   Collector<JSONObject> out) throws Exception {
						String firstVisitDate = firstVisitDateState.value();

						JSONObject common = obj.getJSONObject("common");
						String isNew = common.getString("is_new");
						Long ts = obj.getLong("ts");

						String todayDate = DateFormatUtil.toDate(ts);

						if ("1".equals(isNew)) {
							if (firstVisitDate != null) {
								firstVisitDateState.update(todayDate);
							} else {
								if (!todayDate.equals(firstVisitDate)) {
									common.put("is_new", "0");
								}
							}
						}else if(firstVisitDate == null) {
							String yesterdayDate = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000);
							firstVisitDateState.update(yesterdayDate);
						}

						out.collect(obj);
					}
				});

	}

	private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
		//过滤不完整数据
		return stream
				.filter(json -> {
					JSONObject obj = JSON.parseObject(json);
					return obj != null;
				})
				.map(JSON::parseObject);

	}
}
