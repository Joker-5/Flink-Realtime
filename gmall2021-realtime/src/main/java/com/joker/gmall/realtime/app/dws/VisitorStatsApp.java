package com.joker.gmall.realtime.app.dws;/*
 *项目名: gmall2021-parent
 *文件名: VisitorStatsApp
 *创建者: Joker
 *创建时间:2021/4/5 17:10
 *描述:
访客主题宽表计算
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.bean.VisitorStats;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(KafkaConstant.KAFKA_PARTITION);

        //TODO 1.从 Kafka 的 pv、uv、跳转明细主题中获取数据
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageViewSource =
                MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource =
                MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource =
                MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);
//        pageViewDStream.print("pv-------->");
//        uniqueVisitDStream.print("uv=====>");
//        userJumpDStream.print("uj--------->");

        //TODO 2.对读取的流进行结构转换
        //2.1 转换 pv 流
        SingleOutputStreamOperator<VisitorStats> pageViewStatsDS = pageViewDStream.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 1L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"),
                            jsonObj.getLong("ts"));
                });
        //2.2 转换 uv 流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDS = uniqueVisitDStream.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
                });
        //2.3 转换 sv 流
        SingleOutputStreamOperator<VisitorStats> sessionVisitDS = pageViewDStream.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String json, Context ctx, Collector<VisitorStats> out) {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            VisitorStats visitorStats = new VisitorStats("", "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"));
                            out.collect(visitorStats);
                        }
                    }
                });
        //2.4 转换跳转流
        SingleOutputStreamOperator<VisitorStats> userJumpStatDS = userJumpDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
        });
        //TODO 3.将四条流合并起来
        DataStream<VisitorStats> unionDetailDS = uniqueVisitStatsDS.union(
                pageViewStatsDS,
                sessionVisitDS,
                userJumpStatDS
        );
        //TODO 4.设置水位线
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS =
                unionDetailDS.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).
                                withTimestampAssigner((visitorStats, ts) -> visitorStats.getTs())
                );

//        visitorStatsWithWatermarkDS.print("after union ==>");

        //TODO 5.分组 选取四个维度作为 key , 使用 Tuple4 组合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsTuple4KeyedStream
                =
                visitorStatsWithWatermarkDS
                        .keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                                   @Override
                                   public Tuple4<String, String, String, String> getKey(VisitorStats
                                                                                                visitorStats) throws Exception {
                                       return new Tuple4<>(visitorStats.getVc()
                                               , visitorStats.getCh(),
                                               visitorStats.getAr(),
                                               visitorStats.getIs_new());
                                   }
                               }
                        );
//        visitorStatsTuple4KeyedStream.print("visit -->");
        //TODO 6.开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS
                =
                visitorStatsTuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //TODO 7.聚合统计
        SingleOutputStreamOperator<VisitorStats> visitorStatsDS =
                windowDS.reduce((ReduceFunction<VisitorStats>) (stats1, stats2) -> {
                            //把度量数据两两相加
                            stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                            stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                            stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                            stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                            stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                            //System.out.println("相加成功~~~");
                            return stats1;
                        }
                        , new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String,
                                String>, TimeWindow>() {
                            @Override
                            public void process(Tuple4<String, String, String, String> tuple4, Context context,
                                                Iterable<VisitorStats> visitorStatsIn,
                                                Collector<VisitorStats> visitorStatsOut) {
                                System.out.println("处理时间！！");
                                //补时间字段
                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                for (VisitorStats visitorStats : visitorStatsIn) {
                                    String startDate = simpleDateFormat.format(new Date(context.window().getStart()));
                                    String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));
                                    visitorStats.setStt(startDate);
                                    visitorStats.setEdt(endDate);
                                    visitorStatsOut.collect(visitorStats);
                                }
                            }
                        }
                );
        //TODO 最后的reduce输入不出来
        visitorStatsDS.print("reduce==>");
        env.execute();
    }
}
