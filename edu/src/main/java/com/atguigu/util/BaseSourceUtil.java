package com.atguigu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.SessionSc;
import com.atguigu.common.Constant;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/5 10:18
 */
public class BaseSourceUtil {

    // 从ods_log中获取sc与session_id信息
    public static Table readOdsLog(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String groupId, Long[] startOffsets) {

        SingleOutputStreamOperator<SessionSc> stream = env
                .addSource(FlinkSourceUtil.getKafkaSource(groupId, Constant.TOPIC_ODS_LOG, startOffsets))
                .map(new MapFunction<String, SessionSc>() {
                    @Override
                    public SessionSc map(String json) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(json);
                            JSONObject common = jsonObject.getJSONObject("common");
                            return new SessionSc(common.getString("sid"), common.getLong("sc"));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        return tEnv.fromDataStream(stream);
    }

    public static void readBaseSource(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "create table base_source(" +
                        " `id` bigint, " +
                        " `source_site` string " +
                        ") with (" +
                        " 'connector' = 'jdbc', " +
                        " 'url' = 'jdbc:mysql://hadoop302:3306/gmall', " +
                        " 'username' = 'root', " +
                        " 'password' = '123456', " +
                        " 'table-name' = 'base_source', " +
                        " 'lookup.cache.max-rows' = '10', " +
                        " 'lookup.cache.ttl' = '30 s' " +
                        ")"
        );
    }
}
