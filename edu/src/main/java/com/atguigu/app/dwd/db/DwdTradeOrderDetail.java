package com.atguigu.app.dwd.db;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.BaseSourceUtil;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: Iris_Liu
 * @Description: todo dwd层交易域下单详情事务事实表
 * @Create_time: 2022/7/5 13:06
 */
public class DwdTradeOrderDetail extends BaseSQLApp {

    private static final String APPNAME = "DwdTradeOrderDetail";

    public static void main(String[] args) {
        new DwdTradeOrderDetail().init(
                11113,
                2,
                APPNAME,
                10
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        readOdsDb(tEnv, APPNAME);

        // 从ods_log获取session_id与sc_code
        Long[] startOffsets = new Long[]{};
        Table sessionIdAndSc = BaseSourceUtil.readOdsLog(env, tEnv, APPNAME, startOffsets);
        tEnv.createTemporaryView("session_sc", sessionIdAndSc);
//        tEnv.sqlQuery("select sessionId,sc from session_sc").execute().print();

        // 从base_source获取sc_name
        BaseSourceUtil.readBaseSource(tEnv);
//        tEnv.sqlQuery("select id,source_site from base_source").execute().print();

        // 从ods_db中过滤出order_info,获取province_id与session_id
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " cast(`data`['province_id'] as bigint) province_id, " +
                        " `data`['session_id'] session_id, " +
                        " pt " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='insert' "
        );
        tEnv.createTemporaryView("order_info", orderInfo);
//        tEnv.sqlQuery("select * from order_info").execute().print();

        // 三表join,获取session_id,sc_code,sc_name
        Table joinedSource = tEnv.sqlQuery(
                "select " +
                        " oi.id order_id, " +
                        " oi.province_id province_id, " +
                        " oi.session_id session_id, " +
                        " ss.sc source_code, " +
                        " bs.source_site source_name " +
                        "from order_info oi " +
                        "join session_sc ss on oi.session_id=ss.sessionId " +
                        "join base_source for system_time as of oi.pt as bs on ss.sc=bs.id "
        );
        tEnv.createTemporaryView("join_source", joinedSource);
//        tEnv.sqlQuery("select * from join_source").execute().print();

        // 读取base_province,获取province_name
        BaseSourceUtil.readBaseProvince(tEnv);
//        tEnv.sqlQuery("select * from base_province").execute().print();

        // 从ods_db中过滤出order_detail
        Table orderDetail = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['course_id'] course_id, " +
                        " `data`['course_name'] course_name, " +
                        " `data`['order_id'] order_id, " +
                        " `data`['origin_amount'] split_origin_amount, " +
                        " `data`['coupon_reduce'] split_coupon_reduce, " +
                        " `data`['final_amount'] split_final_amount, " +
                        " `data`['order_status'] order_status, " +
                        " ts * 1000 ts," +
                        " pt " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail' " +
                        "and `type`='insert' "
        );
        tEnv.createTemporaryView("order_detail", orderDetail);
//        tEnv.sqlQuery("select * from order_detail").execute().print();

        // 三表join,获取最终表
        Table dwdTradeOrderDetail = tEnv.sqlQuery(
                "select " +
                        " od.id order_detail_id, " +
                        " od.user_id user_id, " +
                        " od.course_id course_id, " +
                        " od.course_name course_name, " +
                        " od.order_id order_id, " +
                        " cast(js.province_id as string) province_id, " +
                        " bp.name province_name, " +
                        " js.session_id session_id, " +
                        " cast(js.source_code as string) source_code, " +
                        " js.source_name source_name," +
                        " od.split_origin_amount split_origin_amount, " +
                        " od.split_coupon_reduce split_coupon_reduce, " +
                        " od.split_final_amount split_final_amount, " +
                        " od.order_status order_status, " +
                        " ts, " +
                        " current_row_timestamp() row_op_ts " +
                        "from order_detail od " +
                        "join join_source js on od.order_id=js.order_id " +
                        "join base_province for system_time as of od.pt as bp on js.province_id=bp.id "
        );
//        dwdTradeOrderDetail.execute().print();

        // 写到kafka
        tEnv.executeSql(
                "create table dwd_trade_order_detail" +
                        "(" +
                        "order_detail_id string, " +
                        "user_id string, " +
                        "course_id string, " +
                        "course_name string, " +
                        "order_id string, " +
                        "province_id string, " +
                        "province_name string, " +
                        "session_id string, " +
                        "source_code string, " +
                        "source_name string, " +
                        "split_origin_amount string, " +
                        "split_coupon_reduce string, " +
                        "split_final_amount string, " +
                        "order_status string, " +
                        "ts bigint, " +
                        "row_op_ts timestamp_ltz(3) " +
                        ")" +
                        SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );
        dwdTradeOrderDetail.executeInsert("dwd_trade_order_detail");
    }
}
