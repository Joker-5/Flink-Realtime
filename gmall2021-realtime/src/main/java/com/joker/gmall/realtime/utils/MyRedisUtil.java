package com.joker.gmall.realtime.utils;/*
 *项目名: gmall2021-parent
 *文件名: MyRedisUtil
 *创建者: Joker
 *创建时间:2021/3/14 14:51
 *描述:

 */

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MyRedisUtil {

    public static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100); //最大可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong
            jedisPool = new JedisPool(jedisPoolConfig, "hadoop202", 6379, 1000);
        }
        return jedisPool.getResource();
    }

}
