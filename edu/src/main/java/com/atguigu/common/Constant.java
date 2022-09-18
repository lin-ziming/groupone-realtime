package com.atguigu.common;

import com.atguigu.bean.TestPaperExamBean;

public class Constant {
	public static final String KAFKA_BROKERS = "hadoop302:9092,hadoop303:9092,hadoop304:9092";
	public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
	public static final String PHOENIX_URL = "jdbc:phoenix:hadoop302,hadoop303,hadoop304:2181";
	public static final String TOPIC_ODS_DB = "ods_db";
	public static final String TOPIC_ODS_LOG = "ods_log";

	public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
	public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop302:8123/gmall";
	public static final int REDIS_DIM_TTL = 2 * 24 * 60 * 60;  //维度ttl:2天

	public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_APPVIDEO = "dwd_traffic_appvideo";
	public static final String TOPIC_DWD_TRAFFIC_UJ_DETAIL = "dwd_traffic_uj_detail";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_PAY_SUC_DETAIL = "dwd_trade_pay_suc_detail";
	public static final String TOPIC_DWD_TRAFFIC_UV = "dwd_traffic_uv";
	public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
	public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
	public static final String TOPIC_DWD_INTERACTION_COMMENT = "dwd_interaction_comment";
	public static final String TOPIC_DWD_INTERACTION_REVIEW = "dwd_interaction_review";
	public static final String DWS_TRAFFIC_SOURCE_UV_WINDOW = "dws_traffic_source_uv_window";
	public static final String DWS_TRADE_SOURCE_PROVINCE_ORDER_WINDOW = "dws_trade_source_province_order_window";

	public static final String TOPIC_DWD_TEST_SCORE_DETAIL = "dwd_test_score_detail";
	public static final String TOPIC_DWD_TEST_PAPER_DETAIL = "dwd_test_paper_detail";
}
