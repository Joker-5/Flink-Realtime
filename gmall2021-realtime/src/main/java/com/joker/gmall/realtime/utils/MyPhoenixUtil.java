package com.joker.gmall.realtime.utils;/*
 *项目名: gmall2021-parent
 *文件名: MyPhoenixUtil
 *创建者: Joker
 *创建时间:2021/3/14 13:38
 *描述:

 */

import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.common.constant.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MyPhoenixUtil {
    private static Connection conn = null;

    public static void init() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        if (conn == null) {
            init();
        }
        List<T> resultList = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                T rowData = clazz.newInstance();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    BeanUtils.setProperty(rowData, md.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }

}
