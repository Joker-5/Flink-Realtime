package com.joker.gmall.realtime.app.dws;/*
 *项目名: gmall2021-parent
 *文件名: ProvinceStatsSqlApp
 *创建者: Joker
 *创建时间:2021/4/6 12:10
 *描述:
    地区主题统计
    还是因为watermark，测试时要调用两次db!
 */

import com.joker.gmall.realtime.bean.ProvinceStats;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.MyClickHouseUtil;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.table.api.EnvironmentSettings;
import ru.yandex.clickhouse.ClickHouseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 基本环境准备
        //1.1 创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(KafkaConstant.KAFKA_PARTITION);
        /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //1.4 创建Table环境
        EnvironmentSettings setting = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2.把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING" +
                ",province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                "split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR  rowtime  AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        //TODO 3.聚合计算
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code area_code," +
                "province_iso_code iso_code ,province_3166_2_code iso_3166_2 ," +
                "COUNT( DISTINCT  order_id) order_count, sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts " +
                " from  ORDER_WIDE group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        //TODO 4.将动态表转换为数据流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);
        //DataStream<Tuple2<Boolean, ProvinceStats>> provinceStatsDS = tableEnv.toRetractStream(provinceStateTable, ProvinceStats.class);

        provinceStatsDS.print(">>>>");

        //TODO 5.将流中的数据保存到ClickHouse
        provinceStatsDS.addSink(
                MyClickHouseUtil.getJdbcSink(
                        "insert into  province_stats_2021  values(?,?,?,?,?,?,?,?,?,?)"
                )
        );

        env.execute();
    }
}
