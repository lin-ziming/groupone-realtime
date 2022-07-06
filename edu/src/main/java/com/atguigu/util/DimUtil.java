package com.atguigu.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 20:47
 */
public class DimUtil {
    public static JSONObject getDimData(DruidPooledConnection phoenixConn, Jedis jedisPoolClient, String table, String id) {
        //readRedis readPho writeRedis
        JSONObject dim;
        dim = readFromRedis(jedisPoolClient, table, id);
        if (dim == null) {
            dim = readFromPhoenix(phoenixConn, table, id);
            writeToRedis(jedisPoolClient, table, id, dim);
            System.out.println("read from phoenix---" + table + "---" + id);
        } else {
            System.out.println("read from redis---" + table + "---" + id);
        }
        return dim;
    }

    private static JSONObject readFromPhoenix(DruidPooledConnection phoenixConn, String table, String id) {
        String sql = "select * from " + table + " where id = ? ";
        Object[] args = {id};
        List<JSONObject> resultList = null;
        try {
            resultList = JdbcUtil.queryList(phoenixConn, sql, args, JSONObject.class);
        } catch (SQLException e) {
            throw new RuntimeException("sql语句有问题, 请检查sql的拼接是否正常...: " + sql);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("请检查你的无参构造器是否有public权限...");
        } catch (InstantiationException e) {
            // throw new RuntimeException("请给 JSONObject 提供无参构造器");
        } catch (InvocationTargetException e) {
            // throw new RuntimeException("在 JSONObject 对象找不到对应的属性....");
        }

        if (resultList.size() == 0) {
            throw new RuntimeException("没有查到对应的维度数据, 请检查表是否存在, 维度数据是否存在: 表名->" + table + " id->" + id);
        }
        return resultList.get(0);
    }

    private static void writeToRedis(Jedis jedisPoolClient, String table, String id, JSONObject dim) {
        String key = table + ":" + id;
        jedisPoolClient.setex(key, Constant.REDIS_DIM_TTL, dim.toJSONString());
    }

    private static JSONObject readFromRedis(Jedis jedisPoolClient, String table, String id) {
        String key = table + ":" + id;
        JSONObject dim = null;

        String value = jedisPoolClient.get(key);
        if (value != null) {
            dim = JSONObject.parseObject(value);
        }
        return dim;
    }
}
