package com.joker.gmall.realtime.common.constant;/*
 *项目名: gmall2021-parent
 *文件名: GmallConfig
 *创建者: Joker
 *创建时间:2021/3/12 14:51
 *描述:

 */

public class GmallConfig {
    //hbase命名空间
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    //phoenix server配置
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181";
    //phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
}
