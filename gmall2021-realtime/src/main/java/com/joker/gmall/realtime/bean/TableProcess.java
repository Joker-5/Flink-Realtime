package com.joker.gmall.realtime.bean;/*
 *项目名: gmall2021-parent
 *文件名: TableProcess
 *创建者: Joker
 *创建时间:2021/3/8 23:51
 *描述:

 */

import lombok.Data;

@Data
public class TableProcess {
    //动态分流 Sink 常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
