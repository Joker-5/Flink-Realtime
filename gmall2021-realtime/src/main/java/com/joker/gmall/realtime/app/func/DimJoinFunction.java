package com.joker.gmall.realtime.app.func;/*
 *项目名: gmall2021-parent
 *文件名: DimJoinFunction
 *创建者: Joker
 *创建时间:2021/3/14 21:55
 *描述:
    维度数据关联接口
 */

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoinFunction<T> {

    //将结果装配给数据流对象
    void join(T t, JSONObject jsonObj) throws ParseException;

    //从数据流对象中获取主键
    String getKey(T t);
}
