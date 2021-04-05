package com.joker.gmall.realtime.utils;/*
 *项目名: gmall2021-parent
 *文件名: MyMysqlUtil
 *创建者: Joker
 *创建时间:2021/3/8 23:52
 *描述:

 */

import akka.Main;
import com.google.common.base.CaseFormat;
import com.joker.gmall.realtime.bean.TableProcess;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MyMysqlUtil {
    private static final String MYSQL_URL = "jdbc:mysql://hadoop202:3306/gmall_realtime?serverTimezone=UTC&characterEncoding=utf-8&useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";

    /**
     * @param sql                   sql语句
     * @param clazz                 对象所对应的类
     * @param underScoreCaseToCamel 根据代码中对象属性是驼峰与否写入true|false
     * @return 查询对象结果集合
     */
    public static <T> List<T> query(String sql, Class<T> clazz, boolean underScoreCaseToCamel) {
        //TODO 0.关闭资源 try-with-resource
        //TODO 2.建立连接
        //TODO 3.创建db操作对象
        //TODO 4.执行sql
        try (
                Connection connection = DriverManager.getConnection(MYSQL_URL, USER, PASSWORD);
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery();
        ) {
            //TODO 1.注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //TODO 5.处理结果集
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<T> resultList = new ArrayList<>();
            while (resultSet.next()) {
                //反射创建对象
                T obj = clazz.newInstance();
                //注意jdbc下标从1开始
                for (int pos = 1; pos <= metaData.getColumnCount(); pos++) {
                    String propertyName = metaData.getColumnName(pos);
                    if (underScoreCaseToCamel) {
                        //工具类简易转换字段风格
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, propertyName);
                    }
                    //bean工具类简易对象属性赋值
                    BeanUtils.setProperty(obj, propertyName, resultSet.getObject(pos));
                }
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("执行sql查询出错！");
        }
    }

}
