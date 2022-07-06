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

    private JedisPoolUtil(){}

    private static class JedisPoolUtilHolder{
        private static final JedisPoolUtil instance = new JedisPoolUtil();
    }

    public static JedisPoolUtil getJedisPoolInstance() {
        return JedisPoolUtilHolder.instance;
    }

    private final JedisPool jedisPool = new JedisPool(JedisPoolUtil.getJedisPoolConfig(), "hadoop302");

    public Jedis getJedisPoolClient(){

        Jedis client = jedisPool.getResource();
        client.select(1);
        return client;
    }

    private static JedisPoolConfig getJedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMinIdle(2);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxWaitMillis(10 * 1000);
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
