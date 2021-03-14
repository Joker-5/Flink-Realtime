package com.joker.gmall.realtime.utils;/*
 *项目名: gmall2021-parent
 *文件名: DimUtil
 *创建者: Joker
 *创建时间:2021/3/14 14:04
 *描述:
    phoenix工具类再封装
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.EWMA;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.metrics2.util.SampleQuantiles;
import org.glassfish.jersey.server.JSONP;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... columnNameAndValue) {
        StringBuilder whereSql = new StringBuilder(" where ");
        for (int i = 0; i < columnNameAndValue.length; i++) {
            Tuple2<String, String> field = columnNameAndValue[i];
            String name = field.f0;
            String value = field.f1;
            if (i > 0) {
                whereSql.append(" and ");
            }
            whereSql.append(name).append("='").append(value).append("'");
        }
        String sql = "select * from " + tableName + whereSql.toString();
        System.out.println("查询维度sql -> " + sql);
        List<JSONObject> dimInfoList = MyPhoenixUtil.queryList(sql, JSONObject.class);
        JSONObject dimInfoJSONObject = null;
        if (dimInfoList.size() > 0) {
            //根据key：id找到的数据有且仅有一条,故get(0)
            dimInfoJSONObject = dimInfoList.get(0);
        } else {
            System.out.println("未找到维度数据");
        }
        return dimInfoJSONObject;
    }

    //重载简易版
    public static JSONObject getDimInfoNoCache(String tableName, String id) {
        return getDimInfoNoCache(tableName, Tuple2.of("id", id));
    }

    //redis旁路缓存优化
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnNameAndValue) {
        StringBuilder whereSql = new StringBuilder(" where ");
        StringBuilder redisKey = new StringBuilder();
        for (int i = 0; i < columnNameAndValue.length; i++) {
            Tuple2<String, String> field = columnNameAndValue[i];
            String name = field.f0;
            String value = field.f1;
            if (i > 0) {
                whereSql.append(" and ");
                redisKey.append("_");
            }
            whereSql.append(name).append("='").append(value).append("'");
            redisKey.append(value);
        }
        String dimJson = null;
        JSONObject dimInfo = null;
        //redis key
        String key = "dim:" + tableName.toLowerCase() + ":" + redisKey.toString();
        try (Jedis jedis = MyRedisUtil.getJedis()) {
            dimJson = jedis.get(key);
            //缓存命中
            if (dimJson != null) {
                dimInfo = JSON.parseObject(dimJson);
            } else {
                String sql = "select * from " + tableName + whereSql.toString();
                System.out.println("查询维度sql -> " + sql);
                List<JSONObject> dimInfoList = MyPhoenixUtil.queryList(sql, JSONObject.class);
                if (dimInfoList.size() > 0) {
                    //根据key：id找到的数据有且仅有一条,故get(0)
                    dimInfo = dimInfoList.get(0);
                    //数据写入redis缓存
                    jedis.setex(key, 24 * 3600, dimInfo.toJSONString());
                } else {
                    System.out.println("未找到维度数据");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return dimInfo;
    }

    //重载简易版
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("id", id));
    }

    //维度数据更新删除缓存
    public static void deleteCache(String tableName, String id) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
//        System.out.println("redis key -->" + key);
        try (Jedis jedis = MyRedisUtil.getJedis()) {
            jedis.del(key);
//            System.out.println("真的删了！！！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "12"));
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "13"));
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "123"));
    }


}
