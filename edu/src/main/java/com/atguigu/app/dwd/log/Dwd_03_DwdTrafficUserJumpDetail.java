package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.common.Constant;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Dwd_03_DwdTrafficUserJumpDetail extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_03_DwdTrafficUserJumpDetail().init(
            2003,
            2,
            "Dwd_03_DwdTrafficUserJumpDetail_1",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {


        KeyedStream<JSONObject, String> keyedStream = stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"));

        // 1. 先定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
            .<JSONObject>begin("entry1")
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId == null || lastPageId.isEmpty();
                    //                    return Strings.isNullOrEmpty(lastPageId);
                }
            })
            .next("entry2")
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId == null || lastPageId.isEmpty();
                }
            })
            .within(Time.seconds(3));

        // 2. 把模式作用到流上, 得到一个模式流
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);
        // 3. 从模式流中得到匹配到的数据或者超时的数据
        SingleOutputStreamOperator<JSONObject> normalStream = ps.select(
            new OutputTag<JSONObject>("late") {},
            new PatternTimeoutFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject timeout(Map<String, List<JSONObject>> pattern,
                                          long timeoutTimestamp) throws Exception {
                    return pattern.get("entry1").get(0);
                }
            },
            new PatternSelectFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                    return pattern.get("entry1").get(0);
                }
            }
        );

        normalStream.getSideOutput(new OutputTag<JSONObject>("late") {}).union(normalStream)
            .map(JSONAware::toJSONString)
            .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UJ_DETAIL));
    }
}
