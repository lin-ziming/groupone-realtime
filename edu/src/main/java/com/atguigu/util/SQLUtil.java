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
