package com.atguigu.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 16:58
 */
public class JedisPoolUtil {

    private JedisPoolUtil() {
    }

    private volatile static JedisPoolUtil jedisPoolUtil;

    public static JedisPoolUtil getJedisPoolInstance() {
        if (jedisPoolUtil == null) {
            synchronized (JedisPoolUtil.class) {
                if (jedisPoolUtil == null) {
                    jedisPoolUtil = new JedisPoolUtil();
                }
            }
        }
        return jedisPoolUtil;
    }

    private static JedisPool jedisPool = new JedisPool(JedisPoolUtil.getJedisPoolConfig(), "hadoop302", 6379, 20000);
//    private static JedisPool jedisPool = new JedisPool(JedisPoolUtil.getJedisPoolConfig(), "hadoop302");

    public Jedis getJedisPoolClient() {

        //有资源再给jedis对象, 让对象等资源
        Jedis client = null;
        try {
            if (jedisPool != null) {
                client = jedisPool.getResource();
            }
        } catch (Exception e) {
            System.out.println("获取redis失败");
            e.printStackTrace();
        }

        client.select(1);
        return client;
    }

    private static JedisPoolConfig getJedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPoolConfig.setMaxIdle(96);
        jedisPoolConfig.setMinIdle(4);
        jedisPoolConfig.setMaxTotal(96);
        jedisPoolConfig.setMaxWaitMillis(20 * 1000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestOnCreate(true);

        return jedisPoolConfig;
    }

    public static void main(String[] args) {
        Jedis jedisPoolClient = getJedisPoolInstance().getJedisPoolClient();
        String s = jedisPoolClient.get("dim_video_info:44");
        System.out.println(s);

        if (jedisPoolClient != null) {
            jedisPoolClient.close();
        }
    }
}
