package com.atguigu.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.util.DruidPoolUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author shogunate
 * @description phoenix for Tuple2<JSONObject, TableProcess>, generic later
 *              connect druidPool and redisPool later
 * @date 2022/7/5 14:05
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private DruidPooledConnection phoenixJdbcConn;

    @Override
    public void close() throws Exception {
        if (phoenixJdbcConn != null) {
            phoenixJdbcConn.close();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //singleton
        DruidPoolUtil druidPoolInstance = DruidPoolUtil.getDruidPoolInstance();
        DruidDataSource druidDataSource = druidPoolInstance.getDruidDataSource();
        phoenixJdbcConn = druidDataSource.getConnection();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        writeToPhoenix(value);
    }

    private void writeToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {

        //upsert into sinkTableName (c1,c2) values (?,?)
        //how to addbatch exe
        StringBuilder sql = new StringBuilder("upsert into ");
        sql
            .append(value.f1.getSinkTable()).append("(")
            .append(value.f1.getSinkColumns()).append(") values (")
            .append(value.f1.getSinkColumns().replaceAll("[^,]+","?"))
            .append(")");
        System.out.println("sinkSql---"+sql);

        PreparedStatement ps = phoenixJdbcConn.prepareStatement(sql.toString());

        String[] sinkCols = value.f1.getSinkColumns().split(",");
        for (int i = 0; i < sinkCols.length; i++) {

            if(value.f0.containsKey(sinkCols[i])){

                Object o = value.f0.get(sinkCols[i]);
                ps.setString(i+1, o == null ? null : o.toString());
            }else {

                ps.setString(i+1, null);
            }
        }
        // how to addBatch
//        ps.addBatch();

        ps.execute();
        phoenixJdbcConn.commit();
        ps.close();
    }
}
































