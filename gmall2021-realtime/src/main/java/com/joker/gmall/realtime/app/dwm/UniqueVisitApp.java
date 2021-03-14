package com.joker.gmall.realtime.app.dwm;/*
 *项目名: gmall2021-parent
 *文件名: UniqueVisitApp
 *创建者: Joker
 *创建时间:2021/3/13 11:40
 *描述:
    uv计算
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基础环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(KafkaConstant.KAFKA_PARTITION);

        String sourceTopic = "dwd_page_log";
        String sourceGroup = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> rawDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, sourceGroup));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = rawDS.map(JSON::parseObject);
//        jsonObjDS.print("jsonObj --->");
        //TODO 2.核心过滤流程
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByMidDS.filter(
                new RichFilterFunction<JSONObject>() {
                    ValueState<String> lastVisitDateState = null;
                    SimpleDateFormat sdf = null;

                    //初始化设置->获取上次访问时间状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                        if (lastVisitDateState == null) {
                            ValueStateDescriptor<String> lastVisitDateStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                            //日活统计需要设置过期时间1d
                            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                            lastVisitDateStateDescriptor.enableTimeToLive(stateTtlConfig);
                            lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDescriptor);
                        }
                    }

                    //uv统计过滤策略
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }
                        Long ts = jsonObj.getLong("ts");
                        String logDate = sdf.format(new Date(ts));
                        String lastVisitDate = lastVisitDateState.value();
                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                            System.out.println("已访问：lastVisit:" + lastVisitDate + "|| logDate：" + logDate);
                            return false;
                        } else {
                            //未访问，需要更新
                            System.out.println("未访问：lastVisit:" + lastVisitDate + "|| logDate：" + logDate);
                            lastVisitDateState.update(logDate);
                            return true;
                        }
                    }
                }
        ).uid("uvFilter");
        SingleOutputStreamOperator<String> kafkaDS = filterDS.map(JSONAware::toJSONString);
        kafkaDS.print("uv --->");
        //TODO 3.写回到kafka dwm主题
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();
    }
}
