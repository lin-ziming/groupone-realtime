package com.atguigu.common;

public class Constant {
    public static final String KAFKA_BROKERS = "hadoop302:9092,hadoop303:9092,hadoop304:9092";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop302,hadoop303,hadoop304:2181";
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_UV = "dwd_traffic_uv";
    public static final String TOPIC_DWD_TRAFFIC_UJ_DETAIL = "dwd_traffic_uj_detail";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_PRE_PROCESS = "dwd_trade_order_pre_process";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";
    public static final String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";
    public static final String TOPIC_DWD_TOOL_COUPON_GET = "dwd_tool_coupon_get";
    public static final String TOPIC_DWD_TOOL_COUPON_ORDER = "dwd_tool_coupon_order";
    public static final String TOPIC_DWD_TOOL_COUPON_PAY = "dwd_tool_coupon_pay";
    public static final String TOPIC_DWD_INTERACTION_FAVOR_ADD = "dwd_interaction_favor_add";
    public static final String TOPIC_DWD_INTERACTION_COMMENT = "dwd_interaction_comment";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop302:8123/gmall2022";
    public static final int REDIS_DIM_TTL = 2 * 24 * 60 * 60;  //维度ttl:2天
}
