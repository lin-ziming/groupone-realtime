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
 * @Description: todo dwd层交易域支付成功事务事实表
 * @Create_time: 2022/7/4 16:18
 */
public class DwdTradePaySucDetail extends BaseSQLApp {

    private static final String APPNAME = "DwdTradePaySucDetail";

    public static void main(String[] args) {
        new DwdTradePaySucDetail().init(
                11112,
                2,
                APPNAME,
                30 * 60
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        readOdsDb(tEnv, APPNAME);

        // 从ods_db获取order_info,拿到session_id
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        " `data`['session_id'] session_id, " +
                        " `data`['id'] id, " +
                        " pt " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='insert' "
        );
        tEnv.createTemporaryView("order_info", orderInfo);
//        tEnv.sqlQuery("select session_id,id,pt from order_info").execute().print();

        // 从ods_log获取session_id与sc_code
        Long[] startOffsets = new Long[]{0L,0L};
        Table sessionIdAndSc = BaseSourceUtil.readOdsLog(env, tEnv, APPNAME, startOffsets);
        tEnv.createTemporaryView("session_sc", sessionIdAndSc);
//        tEnv.sqlQuery("select sessionId,sc from session_sc").execute().print();

        // 从base_source获取sc_name
        BaseSourceUtil.readBaseSource(tEnv);
//        tEnv.sqlQuery("select id,source_site from base_source").execute().print();

        // 三表join,获取session_id,sc_code,sc_name
        Table joinedSource = tEnv.sqlQuery(
                "select " +
                        " oi.id order_id, " +
                        " oi.session_id session_id, " +
                        " ss.sc source_code, " +
                        " bs.source_site source_name " +
                        "from order_info oi " +
                        "join session_sc ss on oi.session_id=ss.sessionId " +
                        "join base_source for system_time as of oi.pt as bs on ss.sc=bs.id"
        );
        tEnv.createTemporaryView("join_source", joinedSource);
//        tEnv.sqlQuery("select * from join_source").execute().print();

        // 从ods_db获取order_detail
        Table orderDetail = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] order_detail_id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['course_id'] course_id, " +
                        " `data`['course_name'] course_name, " +
                        " `data`['order_id'] order_id, " +
                        " cast(`data`['origin_amount'] as bigint) origin_amount, " +
                        " cast(`data`['coupon_reduce'] as bigint) coupon_reduce, " +
                        " cast(`data`['final_amount'] as bigint) final_amount " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail' " +
                        "and `type`='insert' "
        );
        tEnv.createTemporaryView("order_detail", orderDetail);
//        tEnv.sqlQuery("select * from order_detail").execute().print();

        // 从ods_db中获取payment_info
        Table paymentInfo = tEnv.sqlQuery(
                "select " +
                        " `data`['order_id'] order_id, " +
                        " `data`['payment_type'] payment_type, " + // 数据缺陷: payment_type无法维度退化
                        " `data`['callback_time'] callback_time, " +
                        " ts * 1000 ts " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='payment_info' " +
                        /*"and `type`='update' " +
                        "and `old`['payment_status'] is not null " +*/ // 数据缺陷: 支付完成的订单却是insert
                        "and `data`['payment_status']='1602'");
        tEnv.createTemporaryView("payment_info", paymentInfo);
//        tEnv.sqlQuery("select * from payment_info").execute().print();

        // 三表join,获取最终表
        Table dwdTradePaySucDetail = tEnv.sqlQuery(
                "select " +
                        " od.order_detail_id order_detail_id, " +
                        " pi.order_id order_id, " +
                        " od.user_id user_id, " +
                        " od.course_id course_id, " +
                        " od.course_name course_name, " +
                        " cast(od.origin_amount as string) split_origin_amount, " +
                        " cast(od.coupon_reduce as string) split_coupon_reduce, " +
                        " cast(od.final_amount as string) split_final_amount, " +
                        " pi.payment_type payment_type, " +
                        " pi.callback_time callback_time, " +
                        " js.session_id session_id, " +
                        " cast(js.source_code as string) source_code, " +
                        " js.source_name source_name, " +
                        " pi.ts ts, " +
                        " current_row_timestamp() row_op_ts " +
                        "from payment_info pi " +
                        "join order_detail od on pi.order_id=od.order_id " +
                        "join join_source js on pi.order_id=js.order_id "
        );
//        dwdTradePaySucDetail.execute().print();

        tEnv.executeSql(
                "create table dwd_trade_pay_suc_detail " +
                        "(" +
                        " order_detail_id string, " +
                        " order_id string, " +
                        " user_id string, " +
                        " course_id string, " +
                        " course_name string, " +
                        " split_origin_amount string, " +
                        " split_coupon_reduce string, " +
                        " split_final_amount string, " +
                        " payment_type string, " +
                        " callback_time string, " +
                        " session_id string, " +
                        " source_code string, " +
                        " source_name string, " +
                        " ts bigint, " +
                        " row_op_ts timestamp_ltz(3) " +
                        ")" +
                        SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_PAY_SUC_DETAIL)
        );

        dwdTradePaySucDetail.executeInsert("dwd_trade_pay_suc_detail");
    }
}
