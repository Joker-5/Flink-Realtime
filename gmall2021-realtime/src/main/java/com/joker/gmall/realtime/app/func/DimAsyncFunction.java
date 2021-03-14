package com.joker.gmall.realtime.app.func;/*
 *项目名: gmall2021-parent
 *文件名: DimAsyncFunction
 *创建者: Joker
 *创建时间:2021/3/14 21:39
 *描述:

 */

import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.utils.DimUtil;
import com.joker.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.commons.pool2.PoolUtils;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

public class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    ExecutorService executorService;
    String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void join(T t, JSONObject jsonObj) {

    }

    @Override
    public String getKey(T t) {
        return null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    //异步调用函数
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(
                () -> {
                    long start = System.currentTimeMillis();
                    //从流中获取主键
                    String key = getKey(obj);
                    //根据主键获取维度数据
                    JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                    if (dimInfoJsonObj != null) {
                        join(obj, dimInfoJsonObj);
                    }
                    System.out.println(obj);
                    long end = System.currentTimeMillis();
                    System.out.println("异步耗时 : " + (end - start) + " ms");
                    resultFuture.complete(Arrays.asList(obj));
                }
        );
    }
}
