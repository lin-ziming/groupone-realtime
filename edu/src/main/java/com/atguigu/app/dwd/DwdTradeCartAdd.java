package com.atguigu.app.dwd;

import com.atguigu.app.BaseAppV1;
import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: Iris_Liu
 * @Description: todo dwd层交易域加购事务事实表
 * @Create_time: 2022/7/4 15:22
 */
public class DwdTradeCartAdd extends BaseSQLApp {

    private static final String APPNAME = "DwdTradeCartAdd";

    public static void main(String[] args) {
        new DwdTradeCartAdd().init(
                1111,
                2,
                APPNAME,
                10
        );
    }


    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        readOdsDb(tEnv, APPNAME);
        Table dwdTradeCartAdd = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['course_id'] course_id, " +
                        " `data`['course_name'] course_name, " +
                        " cast(`data`['cart_price'] as bigint) cart_price, " +
                        " ts * 1000 ts " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='cart_info' " +
                        "and `type`='insert' "
        );
        tEnv.executeSql(
                "create table dwd_trade_cart_add " +
                        "(" +
                        " id string, " +
                        " user_id string, " +
                        " course_id string, " +
                        " course_name string, " +
                        " cart_price bigint, " +
                        " ts bigint" +
                        ")" +
                        SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );
        dwdTradeCartAdd.executeInsert("dwd_trade_cart_add");
    }
}
