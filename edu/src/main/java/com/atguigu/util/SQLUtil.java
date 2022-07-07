package com.atguigu.util;

import com.atguigu.common.Constant;

public class SQLUtil {
    /**
     * 可以在这个方法体内，直接指定每个分区的起始消费偏移量offsets;
     * 需要自己放开注释，并手动填写
     * @param topic
     * @param groupId
     * @return
     */
    public static String getKafkaSourceDDL(String topic, String groupId) {
        return "with(\n" +
                " 'connector'='kafka', \n" +
                " 'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "',\n" +
                " 'properties.group.id'='" + groupId + "', \n" +
                /*" 'properties.auto.offset.reset' = 'earliest', " +*/ // 消费者组第一次消费从头开始,后续消费从上次提交的offsets开始
//                " 'scan.startup.mode' = 'earliest-offset', " + // 永远从头开始消费
                " 'format'='json', \n" +
                " 'topic'='" + topic + "'\n" +

                //起始消费offsets
//                " ,'scan.startup.mode' = 'specific-offsets' \n" +
//                " ,'scan.startup.specific-offsets' = 'partition:0,offset:30000;" +
//                                                     "partition:1,offset:30000'\n" +
                ")";
    }

    public static String getKafkaSinkDDL(String topic) {
        return "with(\n" +
                " 'connector' = 'kafka', \n" +
                " 'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', \n" +
                " 'format' = 'json', \n" +
                " 'topic' = '" + topic + "' \n" +
                ")";
    }

    public static String getUpsertKafkaSinkDDL(String topic) {
        return "with(\n" +
                " 'connector' = 'upsert-kafka', \n" +
                " 'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', \n" +
                " 'key.format' = 'json', \n" +
                " 'value.format' = 'json', \n" +
                " 'topic' = '" + topic + "' \n" +
                ")";
    }
}
