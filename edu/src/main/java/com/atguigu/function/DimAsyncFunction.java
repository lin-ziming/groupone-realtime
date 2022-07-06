package com.atguigu.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.DimUtil;
import com.atguigu.util.DruidPoolUtil;
import com.atguigu.util.JedisPoolUtil;
import com.atguigu.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.SQLException;
import java.util.Collections;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 19:18
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    public abstract String getTable();

    public abstract String getId(T input);

    public abstract void addDim(T input, JSONObject dim);


    private ThreadPoolUtil threadPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPoolInstance();
    }


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPool.executor(new Runnable() {

            @Override
            public void run() {
                //phoConn and redis conn
                DruidDataSource druidDataSource = DruidPoolUtil.getDruidPoolInstance().getDruidDataSource();
                DruidPooledConnection phoenixConn = null;
                Jedis jedisPoolClient = null;
                JSONObject dim = null;

                try {
                    phoenixConn = druidDataSource.getConnection();
                } catch (SQLException e) {
                    throw new RuntimeException("Can't access Phoenix");
                }

                try {
                    jedisPoolClient = JedisPoolUtil.getJedisPoolInstance().getJedisPoolClient();
                    dim = DimUtil.getDimData(phoenixConn, jedisPoolClient, getTable(), getId(input));
                } catch (Exception e) {
                    System.out.println("Check whether the redis is configured");
                    e.printStackTrace();
                }

                //input getDimData from both
                addDim(input, dim);

                //result collections singleton input
                resultFuture.complete(Collections.singletonList(input));

                //close
                if (phoenixConn != null) {
                    try {
                        phoenixConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if (jedisPoolClient != null) {
                    jedisPoolClient.close();
                }
            }
        });
    }
}
