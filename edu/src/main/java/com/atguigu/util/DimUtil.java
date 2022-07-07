package com.atguigu.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 20:47
 */
public class DimUtil {
    public static JSONObject getDimData(DruidPooledConnection phoenixConn, Jedis jedisPoolClient, String table, String id) {

        JSONObject dim = readFromRedis(jedisPoolClient, table, id);
        if (dim == null) {
            dim = readFromPhoenix(phoenixConn, table, id);
//            System.out.println("read from phoenix---" + table + "---" + id + "dim+++" + dim);
            writeToRedis(jedisPoolClient, table, id, dim);
        }else {
//            System.out.println("read from redis---" + table + "---" + id);
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

        //key 不存在再写入值nx, 再赋过期时间ex, 一起设置防止分布式锁问题
        jedisPoolClient.set(
            key,
            dim.toJSONString(),
            SetParams.setParams().nx()
                //过期时间加入随机值[0,10), 防止同一时间过期造成堵塞
                .ex(Constant.REDIS_DIM_TTL + new Random().nextInt(10)));

        //用完jedis在这立刻关掉
        jedisPoolClient.close();

        //弃用
//        jedisPoolClient.setex(key, Constant.REDIS_DIM_TTL, dim.toJSONString());
    }

    private static JSONObject readFromRedis(Jedis jedisPoolClient, String table, String id) {
        String key = table + ":" + id;
//        JSONObject dim = null;
        String value;

        //exist(key) 存在再取值
        if (jedisPoolClient.exists(key)) {

            value = jedisPoolClient.get(key);

            //用完jedis不能在这关掉
//            jedisPoolClient.close();

            if (value != null) {
                return JSONObject.parseObject(value);
            }
        }
        return null;
    }
}
