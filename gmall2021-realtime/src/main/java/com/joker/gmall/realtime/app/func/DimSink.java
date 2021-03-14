package com.joker.gmall.realtime.app.func;/*
 *项目名: gmall2021-parent
 *文件名: DimSink
 *创建者: Joker
 *创建时间:2021/3/12 17:37
 *描述:

 */

import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.common.constant.GmallConfig;
import com.joker.gmall.realtime.utils.DimUtil;
import com.joker.gmall.realtime.utils.MyMysqlUtil;
import com.sun.org.apache.bcel.internal.generic.DADD;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection connection = null;

    @Override
    //初始化时建立phoenix连接
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    //提交数据至phoenix
    @Override
    public void invoke(JSONObject jsonObj, Context ctx) throws Exception {
        String tableName = jsonObj.getString("sink_table");
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            String upsertSql = genUpsertSql(tableName.toUpperCase(), dataJsonObj);
            System.out.println("upsert sql为 : " + upsertSql);
            try (PreparedStatement ps = connection.prepareStatement(upsertSql)) {
                ps.executeUpdate();
                //坑!phoenix必须要手动提交事务！！！->看connection实现的源码
                connection.commit();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("向phoenix插入数据异常!!!");
            }
        }
        //维度数据发生变化则清空该数据在redis中的缓存
        if (jsonObj.getString("type").equals("update")
                || jsonObj.getString("type").equals("delete")) {
//            System.out.println("缓存删除执行成功~~");
            DimUtil.deleteCache(tableName, dataJsonObj.getString("id"));
        }
    }

    //生成upsertSql语句
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();
        String updateSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "("
                + StringUtils.join(keys, ",") + ")";
        //注意此处因为在phoenix中所有字段都是varchar类型，所以值要赢''括起来转为该类型
        String valuesSql = " values ('" + StringUtils.join(values, "','") + "')";
        return updateSql + valuesSql;
    }
}
