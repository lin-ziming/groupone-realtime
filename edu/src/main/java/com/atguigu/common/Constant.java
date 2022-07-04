package com.atguigu.common;

public class Constant {
    public static final String KAFKA_BROKERS = "hadoop302:9092,hadoop303:9092,hadoop304:9092";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop302,hadoop303,hadoop304:2181";
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_ODS_LOG = "ods_log";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop302:8123/gmall2022";
    public static final int REDIS_DIM_TTL = 2 * 24 * 60 * 60;  //维度ttl:2天
}
