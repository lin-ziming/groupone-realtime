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
 * @Description: todo dwd层交易域加购事务事实表
 * @Create_time: 2022/7/4 15:22
 */

// 待优化: 重复的session_id对应sc,不应查询多次,采用redis解决
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

        Long[] startOffsets = new Long[]{0L,0L};
        Table sessionIdAndSc = BaseSourceUtil.readOdsLog(env, tEnv, APPNAME, startOffsets);
        tEnv.createTemporaryView("session_sc", sessionIdAndSc);

        BaseSourceUtil.readBaseSource(tEnv);

        // 从ods_db中过滤cart_info
        Table cartInfo = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['course_id'] course_id, " +
                        " `data`['course_name'] course_name, " +
                        " cast(`data`['cart_price'] as bigint) cart_price, " +
                        " `data`['session_id'] session_id, " +
                        " ts * 1000 ts," +
                        " pt " +
                        "from ods_db " +
                        "where `database`='gmall' " +
                        "and `table`='cart_info' " +
                        "and `type`='insert' "
        );
        tEnv.createTemporaryView("cart_info", cartInfo);

        // cart_info join session_sc获取sc_code join base_source获取sc_name
        Table dwdTradeCartAdd = tEnv.sqlQuery(
                "select " +
                        " ci.id id, " +
                        " user_id, " +
                        " course_id, " +
                        " course_name, " +
                        " cart_price, " +
                        " session_id," +
                        " cast(sc as string) source_code," +
                        " source_site source_name, " +
                        " ts," +
                        " current_row_timestamp() row_op_ts " +
                        "from cart_info ci " +
                        "join session_sc ss on ci.session_id=ss.sessionId " +
                        "join base_source for system_time as of ci.pt as bs on ss.sc=bs.id"
        );

        tEnv.executeSql(
                "create table dwd_trade_cart_add " +
                        "(" +
                        " id string, " +
                        " user_id string, " +
                        " course_id string, " +
                        " course_name string, " +
                        " cart_price bigint, " +
                        " session_id string, " +
                        " source_code string, " +
                        " source_name string, " +
                        " ts bigint, " +
                        " row_op_ts timestamp_ltz(3) " +
                        ")" +
                        SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );
        dwdTradeCartAdd.executeInsert("dwd_trade_cart_add");
    }
}
