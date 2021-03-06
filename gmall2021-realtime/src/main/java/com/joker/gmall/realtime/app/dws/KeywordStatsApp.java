package com.joker.gmall.realtime.app.dws;/*
 *项目名: gmall2021-parent
 *文件名: KeywordStatsApp
 *创建者: Joker
 *创建时间:2021/4/6 12:32
 *描述:
    搜索关键词实现
 */

import com.joker.gmall.realtime.app.udf.KeywordUDTF;
import com.joker.gmall.realtime.bean.KeywordStats;
import com.joker.gmall.realtime.common.constant.GmallConstant;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.MyClickHouseUtil;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.table.api.EnvironmentSettings;
import ru.yandex.clickhouse.ClickHouseUtil;
import javassist.compiler.ast.Keyword;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
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

        //TODO 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.创建动态表
        //3.1 声明主题以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
        String groupId = "keywordstats_app_group";
        //3.2建表
        tableEnv.executeSql(
                "CREATE TABLE page_view (" +
                        " common MAP<STRING, STRING>," +
                        " page MAP<STRING, STRING>," +
                        " ts BIGINT," +
                        " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
        );
        //TODO 4.从动态表中查询数据  --->联通双卡双待-> [联通, 双,卡, 双,待]
        Table fullwordTable = tableEnv.sqlQuery(
                "select page['item'] fullword,rowtime " +
                        " from page_view " +
                        " where page['page_id']='good_list' and page['item'] IS NOT NULL"
        );
        //TODO 5.利用自定义函数  对搜索关键词进行拆分
        Table keywordTable = tableEnv.sqlQuery(
                "SELECT keyword, rowtime " +
                        "FROM  " + fullwordTable + "," +
                        "LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)"
        );
        //TODO 6.分组、开窗、聚合
        Table reduceTable = tableEnv.sqlQuery(
                "select keyword,count(*) ct,  '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," +
                        "UNIX_TIMESTAMP()*1000 ts from " + keywordTable +
                        " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"
        );

        //TODO 7.转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(reduceTable, KeywordStats.class);

        keywordStatsDS.print(">>>>");

        //TODO 8.写入到ClickHouse
        keywordStatsDS.addSink(
                MyClickHouseUtil.getJdbcSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );

        env.execute();
    }
}
