package com.atguigu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil2 {
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String dimTable, String id) {
        //select * from t where id=?
        String sql = "select * from " + dimTable + " where id=?";
        Object[] args = {id};
        List<JSONObject> result = null;
        try {
            result = JdbcUtil.<JSONObject>queryList(phoenixConn, sql, args, JSONObject.class);
        } catch (SQLException e) {
            
            throw new RuntimeException("sql语句有问题, 请检查sql的拼接是否正常...: " + sql);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("请检查你的无参构造器是否有public权限...");
        } catch (InstantiationException e) {
            // throw new RuntimeException("请给 JSONObject 提供无参构造器");
        } catch (InvocationTargetException e) {
            // throw new RuntimeException("在 JSONObject 对象找不到对应的属性....");
        }
        
        if (result.size() == 0) {
            throw new RuntimeException("没有查到对应的维度数据, 请检查表是否存在, 维度数据是否存在: 表名->" + dimTable + " id->" + id);
        }
        return result.get(0);  // 获取维度数据
    }
    
    public static JSONObject readDim(Jedis redisClient, Connection phoenixConn, String dimTable, String id) {
        
        // 1. 从redis读取维度数据
        JSONObject dim = readFromRedis(redisClient, dimTable, id);
        // 2. 如果存在则把读到的维度数据返回
        if (dim == null) {
            // 3. 如果不存在则从phoenix读取, 把读取到数据再存入到redis中, 然后在把维度数据返回
            dim = readDimFromPhoenix(phoenixConn, dimTable, id);
            
            // 把读取到数据再存入到redis中
            writeToRedis(redisClient, dimTable, id, dim);
            System.out.println(dimTable + " " + id + " 查询的数据库....");
        }else{
            System.out.println(dimTable + " " + id + " 查询的缓存 ....");
        }
        
        
        return dim;
    }
    
    //TODO
    private static void writeToRedis(Jedis redisClient, String dimTable, String id, JSONObject dim) {
        String key = dimTable + ":" + id;
        
        /*redisClient.set(key, dim.toJSONString());
        redisClient.expire(key, Constant.DIM_TTL);*/
        
        redisClient.setex(key, Constant.REDIS_DIM_TTL, dim.toJSONString());
        
    }
    
    // TODO
    
    private static JSONObject readFromRedis(Jedis redisClient, String dimTable, String id) {
        String key = dimTable + ":" + id;
        String dimJson = redisClient.get(key);
        
        JSONObject dim = null;
        
        if (dimJson != null) {
            dim = JSON.parseObject(dimJson);
        }
        return dim;
    }
}
