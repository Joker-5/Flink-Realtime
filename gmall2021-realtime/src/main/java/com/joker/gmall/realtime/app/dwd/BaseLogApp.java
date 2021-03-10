package com.joker.gmall.realtime.app.dwd;/*
 *项目名: gmall2021-parent
 *文件名: BaseLogApp
 *创建者: Joker
 *创建时间:2021/3/7 13:25
 *描述:
    From kafka ods用户行为日志获取
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

import  com.joker.gmall.realtime.common.constant.KafkaConstant;

public class BaseLogApp {

    private static final String BASE_LOG_TOPIC = "ods_base_log";
    private static final String START_TOPIC = "dwd_start_log";
    private static final String DISPLAY_TOPIC = "dwd_display_log";
    private static final String PAGE_TOPIC = "dwd_page_log";

    public static void main(String[] args) throws Exception {
        //TODO 1. 基础环境设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度与kafka partition一致
        env.setParallelism(KafkaConstant.KAFKA_PARTITION);
        //CheckPoint设置
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/baseLogApp"));
//        System.setProperty("HADOOP_USER_NAME", "root");
        //TODO 2. 从kafka中获取数据
        String groupId = "ods_dwd_base_app_log";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(BASE_LOG_TOPIC, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //TODO 3.数据类型转换->预处理: string -> json
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS
                .map((MapFunction<String, JSONObject>) JSON::parseObject);
        //TODO 4.识别新老访客并对数据进行修复
        //按照mid进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjectDS.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );
        //识别数据是新老用户并对数据进行修复
        SingleOutputStreamOperator<JSONObject> midWithNewFlagDS = midKeyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    //保存首次访问日期的状态
                    private ValueState<String> firstVisitDataState;
                    //日期数据格式化
                    private SimpleDateFormat simpleDateFormat;

                    //open 预处理
                    @Override
                    public void open(Configuration parameters) {
                        //初始化数据
                        firstVisitDataState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("newMidDateState", String.class)
                        );
                        simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        Long ts = jsonObject.getLong("ts");
                        //新访客-> 1 老访客-> 0
                        if ("1".equals(isNew)) {
                            //新访客状态获取
                            String newMidDate = firstVisitDataState.value();
                            String tsDate = simpleDateFormat.format(new Date(ts));
                            //新访客状态非空(说明已经访问过了) && 两次状态不同(说明不是同一天登录)说明标记错误，需要修复
                            if (newMidDate != null && newMidDate.length() != 0) {
                                if (!newMidDate.equals(tsDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                //是新访客，需要更新状态
                                firstVisitDataState.update(tsDate);
                            }
                        }
                        return jsonObject;
                    }
                }
        );
        //TODO 5.对日志数据进行分流
        //利用侧输出流将数据拆分
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = midWithNewFlagDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context ctx, Collector<String> collector) {
                        JSONObject startObj = jsonObject.getJSONObject("start");
                        //写入到kafka需要json -> string
                        //启动日志输入到侧输出流
                        String dataStr = jsonObject.toString();
                        if (startObj != null && startObj.size() > 0) {
                            ctx.output(startTag, dataStr);
                        } else {
                            //曝光日志
                            JSONArray displays = jsonObject.getJSONArray("displays");
                            if (displays != null && displays.size() > 0) {
                                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                //拆分曝光数据输出到侧输出流
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject obj = displays.getJSONObject(i);
                                    obj.put("page_id", pageId);
                                    ctx.output(displayTag, obj.toString());
                                }
                                //页面日志输出到主流
                            } else {
                                collector.collect(dataStr);
                            }
                        }
                    }
                }
        );
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
//        pageDS.print("page--->");
//        startDS.print("start--->");
//        displayDS.print("display--->");
        //TODO 6.分流数据写入kafka
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(START_TOPIC);
        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(DISPLAY_TOPIC);
        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(PAGE_TOPIC);
        pageDS.addSink(pageSink);
        startDS.addSink(startSink);
        displayDS.addSink(displaySink);

        env.execute();
    }
}
