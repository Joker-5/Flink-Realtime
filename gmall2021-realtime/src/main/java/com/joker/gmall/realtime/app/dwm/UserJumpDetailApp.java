package com.joker.gmall.realtime.app.dwm;/*
 *项目名: gmall2021-parent
 *文件名: UserJumpDetailApp
 *创建者: Joker
 *创建时间:2021/3/13 23:35
 *描述:
    用户跳出记录统计
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基础环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(KafkaConstant.KAFKA_PARTITION);
//        env.setParallelism(1);
        //TODO 2.from kafka 读取数据并转换数据类型
        String sourceTopic = "dwd_page_log";
        String sourceGroup = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> rawDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, sourceGroup));
        //测试数据
//        DataStream<String> ds = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":150000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":300000} "
//                );
        //TODO 改了一下测试用
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = ds.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = rawDS.map(JSON::parseObject);
        //TODO 3.设置时间语义->指定事件时间字段
        SingleOutputStreamOperator<JSONObject> timestampsAndWatermarksDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<JSONObject>)
                                (jsonObj, recordTimestamp) ->
                                        jsonObj.getLong("ts")));

        //TODO 4.用户分组统计
        KeyedStream<JSONObject, String> keyedByMidDS = timestampsAndWatermarksDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
//        keyedByMidDS.print("TEST --->");
        //TODO 5.复杂事件处理 -> Flink CEP表达式
        Pattern<JSONObject, JSONObject> jumpFilterPattern = Pattern.<JSONObject>begin("first")
                .where(
                        new SimpleCondition<JSONObject>() {
                            //条件1:用户进入的第一个页面 ->没有last_page_id
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                                System.out.println("first in :" + lastPageId);
                                return lastPageId == null || lastPageId.length() == 0;
                            }
                        }
                )
                .next("next")
                .where(
                        new SimpleCondition<JSONObject>() {
                            //条件2:在页面停留超过指定事件就退出
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                System.out.println("next:" + pageId);
                                return pageId != null && pageId.length() > 0;
                            }
                        }
                ).within(Time.seconds(10));
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedByMidDS, jumpFilterPattern);
        //TODO 6.根据CEP分流
        final OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> filterDS = patternDS.flatSelect(timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {

                    @Override
                    //过滤出的超时数据写入到超时流中->因tag会在写入时自动加入到数据中，故无需显式标记
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        for (JSONObject jsonObject : pattern.get("first")) {
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        //不需要未过滤数据,故不需要任何处理
                    }
                }
        );
        //TODO 7.分流数据写入kafka
        DataStream<String> kafkaDS = filterDS.getSideOutput(timeoutTag);
        kafkaDS.print("timeout data --->");
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();
    }
}
