package com.joker.gmall.realtime.app.func;/*
 *项目名: gmall2021-parent
 *文件名: TableProcessFunction
 *创建者: Joker
 *创建时间:2021/3/10 21:54
 *描述:
配置表处理函数
 */

import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.bean.TableProcess;
import com.joker.gmall.realtime.utils.MyMysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.*;


public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {
    private OutputTag<JSONObject> outputTag;
    //内存存储配置信息对象
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();
    //内存存储存在的hbase表
    private Set<String> hbaseTableExistSet = new HashSet<>();
    //phoenix连接
    private Connection connection = null;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //初始化连接时调用的生命周期方法
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //TODO phoenix连接配置
        connection = DriverManager.getConnection("TODO");
        //定时任务，每5s去mysql更新一次配置信息
        Timer timer = new Timer();
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        initProcessTableMap();
                    }
                }
                , 5000, 5000);
    }

    //读取mysql配置表信息存入内存中
    private void initProcessTableMap() {
        System.out.println("更新读取的配置信息~~~");
        List<TableProcess> tableProcessList = MyMysqlUtil.query("select * from table_process", TableProcess.class, true);
        for (TableProcess process : tableProcessList) {
            //依次读取配置信息
            //操作类型
            String operateType = process.getOperateType();
            //列名
            String sinkColumns = process.getSinkColumns();
            //拓展信息
            String sinkExtend = process.getSinkExtend();
            //主键
            String sinkPk = process.getSinkPk();
            //结果表名
            String sinkTable = process.getSinkTable();
            //sink类型 hbase || kafka
            String sinkType = process.getSinkType();
            //源表名
            String sourceTable = process.getSourceTable();
            //phoenix主键名
            String key = sourceTable + ":" + operateType;
            //写入缓存
            tableProcessMap.put(key, process);
            //判断hbase中表是否存在从而决定是否插入
            if ("insert".equals(operateType) && "hbase".equals(sinkType)) {
                //表明尚未包含该表，需要创建
                if (hbaseTableExistSet.add(sourceTable)) {
                    checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                }
            }

        }
        if (tableProcessMap == null || tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息！！！");
        }
    }

    //检查hbase中是否创建了当前表并选择是否创建
    private void checkTable(String tableName, String fields, String pk, String ext) {
        //指定字段不存在则赋予默认值
        if (pk == null) {
            pk = "id";
        }
        if (ext == null) {
            ext = "";
        }
        StringBuilder sql = new StringBuilder("create table if not exists ");
        //TODO 拼接sql
    }

    //每个元素进行的处理
    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {

    }
}
