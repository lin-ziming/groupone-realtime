package com.atguigu.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author lzc
 * @Date 2022/6/29 9:14
 */
public class RedisUtil {

    static {

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(10);  // 运行最大空闲连接
        poolConfig.setMinIdle(2);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnCreate(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(poolConfig, "hadoop302");
    }

    private static final JedisPool pool;
    
    public static Jedis getRedisClient() {

        Jedis client = pool.getResource();
        client.select(1);

        return client;
    }
}
