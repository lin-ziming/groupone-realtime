package com.atguigu.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.Executor;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 19:18
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    private DruidDataSource druidDataSource;

    public abstract String getTable();

    public abstract String getId(T input);

    public abstract void addDim(T input, JSONObject dim);


    private Executor executor;


    @Override
    public void open(Configuration parameters) throws Exception {
        executor = ThreadPoolUtil.getThreadPoolInstance().getThreadPool();

        druidDataSource = DruidPoolUtil.getDruidPoolInstance().getDruidDataSource();

    }


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        executor.execute(new Runnable() {

            @Override
            public void run() {
                //phoConn and redis conn
                DruidPooledConnection phoenixConn = null;

//                Jedis jedisPoolClient = RedisUtil.getRedisClient();
                Jedis jedisPoolClient = JedisPoolUtil.getJedisPoolInstance().getJedisPoolClient();

//                try {
//                    jedisPoolClient = RedisUtil.getRedisClient();
//                } catch (Exception e) {
//                    System.out.println("Redis time out");
//                    e.printStackTrace();
//                }

                try {
                    phoenixConn = druidDataSource.getConnection();
                } catch (SQLException e) {
                    throw new RuntimeException("Can't access Phoenix");
                }

                JSONObject dim = DimUtil.getDimData(phoenixConn, jedisPoolClient, getTable(), getId(input));
//                JSONObject dim = DimUtil2.readDim(jedisPoolClient, phoenixConn,  getTable(), getId(input));

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
