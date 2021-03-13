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
import com.joker.gmall.realtime.common.constant.GmallConfig;
import com.joker.gmall.realtime.utils.MyMysqlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
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
        //phoenix连接配置
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
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

    //加载配置信息->读取mysql配置表信息存入内存中
    private void initProcessTableMap() {
        //System.out.println("更新读取的配置信息~~~");
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
        StringBuilder sql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA
                + "." + tableName + "(");
        //将列做切分,并拼接至建表语句 SQL 中
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            if (pk.equals(field)) {
                sql.append(field).append(" varchar primary key ");
            } else {
                sql.append("info.").append(field).append(" varchar");
            }
            if (i < fieldsArr.length - 1) {
                sql.append(",");
            }
        }
        sql.append(")");
        sql.append(ext);
        try {
            //执行建表语句在 Phoenix 中创建表
            System.out.println(sql);
            PreparedStatement ps = connection.prepareStatement(sql.toString());
            ps.execute();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败！！！");
        }
    }


    //每个元素进行的处理
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> collector) throws Exception {
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        // 坑！！如果是使用 Maxwell 的初始化功能，他会将mysql之前的数据读取进来
        // 那么 type 类型为 bootstrap-insert
        // 将这里也标记为 insert,不然可能后续运行时会报错
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }
        //获取配置表信息
        if (tableProcessMap != null && tableProcessMap.size() != 0) {
            //注意主键格式要和上面设置的一样
            String key = table + ":" + type;
            TableProcess tableProcess = tableProcessMap.get(key);
            if (tableProcess != null) {
                //业务数据进一步处理
                jsonObj.put("sink_table", tableProcess.getSinkTable());
                String sinkColumns = tableProcess.getSinkColumns();
                if (sinkColumns != null && sinkColumns.length() > 0) {
                    //对业务数据json data字段数据进行过滤
                    filterColumn(jsonObj.getJSONObject("data"), sinkColumns);
                } else {
                    System.out.println("No this key : " + key);
                }
            }
            //hbase维度数据输入到侧输出流
            //System.out.println("开始分流~~~~");
            if (tableProcess != null && TableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(tableProcess.getSinkType())) {
                ctx.output(outputTag, jsonObj);
                //主流事实表数据输出到kafka
            } else if (tableProcess != null && TableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(tableProcess.getSinkType())) {
                collector.collect(jsonObj);
            }
        }

    }

    //过滤JSON中的多余字段
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] cols = StringUtils.split(sinkColumns, ",");
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> columnList = Arrays.asList(cols);
        //精简写法删除元素
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}
