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

    private volatile static JedisPoolUtil jedisPoolUtil;

    public static JedisPoolUtil getJedisPoolInstance(){
        if (jedisPoolUtil == null){
            synchronized (JedisPoolUtil.class){
                if (jedisPoolUtil == null){
                    jedisPoolUtil = new JedisPoolUtil();
                }
            }
        }
        return jedisPoolUtil;
    }

    private static JedisPool jedisPool = new JedisPool(JedisPoolUtil.getJedisPoolConfig(), "hadoop302", 6379, 60000);

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
        jedisPoolConfig.setMaxWaitMillis(60 * 1000);
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
