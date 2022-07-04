package com.atguigu.app.dwd;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
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
        new DwdTradeCartAdd().init(
                1112,
                2,
                APPNAME,
                30 * 60
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        readOdsDb(tEnv, APPNAME);

        Table orderDetail = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] order_detail_id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['course_id'] course_id, " +
                        " `data`['course_name'] course_name, " +
                        " `data`['order_id'] order_id, " +
                        " cast(`data`['origin_amount'] as bigint) origin_amount, " +
                        " cast(`data`['coupon_reduce'] as bigint) coupon_reduce, " +
                        " cast(`data`['final_amount'] as bigint) final_amount, " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail' " +
                        "and `type`='insert' "
        );
        tEnv.createTemporaryView("order_detail", orderDetail);

        Table paymentInfo = tEnv.sqlQuery(
                "select " +
                        "`data`['order_id'] order_id, " +
                        "`data`['payment_type'] payment_type, " + // 数据缺陷: payment_type无法维度退化
                        "`data`['callback_time'] callback_time, " +
                        "ts * 1000 ts " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='payment_info' " +
                        /*"and `type`='update' " +
                        "and `old`['payment_status'] is not null " +*/ // 数据缺陷: 支付完成的订单却是insert
                        "and `data`['payment_status']='1602'");
        tEnv.createTemporaryView("payment_info", paymentInfo);

        Table dwdTradePaySucDetail = tEnv.sqlQuery(
                "select " +
                        " od.order_detail_id order_detail_id, " +
                        " pi.order_id order_id, " +
                        " od.user_id user_id, " +
                        " od.course_id course_id, " +
                        " od.course_name course_name, " +
                        " od.origin_amount split_origin_amount, " +
                        " od.coupon_reduce split_coupon_reduce, " +
                        " od.final_amount split_final_amount, " +
                        " pi.payment_type payment_type, " +
                        " pi.callback_time callback_time, " +
                        " pi.ts " +
                        "from payment_info pi" +
                        "join order_detail od on pi.order_id=od.order_id "
        );

        tEnv.executeSql(
                "create table dwd_trade_pay_suc_detail " +
                        "(" +
                        " order_detail_id string, " +
                        " order_id string, " +
                        " user_id string, " +
                        " course_id string, " +
                        " course_name string, " +
                        " split_origin_amount bigint, " +
                        " split_coupon_reduce bigint, " +
                        " split_final_amount bigint, " +
                        " payment_type, " +
                        " callback_time, " +
                        " ts " +
                        ")" +
                        SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_PAY_SUC_DETAIL)
        );

        dwdTradePaySucDetail.executeInsert("dwd_trade_pay_suc_detail");
    }
}
