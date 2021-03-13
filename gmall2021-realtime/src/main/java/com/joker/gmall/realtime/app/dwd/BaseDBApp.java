package com.joker.gmall.realtime.app.dwd;/*
 *项目名: gmall2021-parent
 *文件名: BaseDBApp
 *创建者: Joker
 *创建时间:2021/3/8 22:06
 *描述:
    From Kafka读取业务db数据发送到dwd层
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.app.func.DimSink;
import com.joker.gmall.realtime.app.func.TableProcessFunction;
import com.joker.gmall.realtime.bean.TableProcess;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    private static final String BASE_DB_TOPIC_NAME = "ods_base_db_m";
    private static final String BASE_DB_GROUP_ID = "ods_base_group";

    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(KafkaConstant.KAFKA_PARTITION);
        //TODO 2.接收Kafka数据并清洗空|脏数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(BASE_DB_TOPIC_NAME, BASE_DB_GROUP_ID);
        System.out.println("连接成功~");
        DataStreamSource<String> stringDS = env.addSource(kafkaSource);
        //DS type : String -> Json
        DataStream<JSONObject> jsonDS = stringDS.map(JSON::parseObject);
        //过滤脏数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(json -> json.getJSONObject("data") != null
                && json.getString("table") != null
                && json.getString("data").length() >= 3);
        //TODO 3.动态分流:事实表放主流入Kafka，维度表放侧流到HBase
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        //主流
        SingleOutputStreamOperator<JSONObject> kafkaDS = filterDS.process(
                new TableProcessFunction(hbaseTag)
        );
        //侧流
        //filterDS.print("原始数据--->");
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        kafkaDS.print("事实数据--->");
        hbaseDS.print("维度数据--->");
        //TODO 4.将维度数据保存到phoenix对应的维度表中
        hbaseDS.addSink(new DimSink());
        //TODO 5.将事实数据写回到kafka dwd层
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("open kafka sink~~");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                String topic = jsonObj.getString("sink_table");
                JSONObject data = jsonObj.getJSONObject("data");
                return new ProducerRecord<>(topic, data.toString().getBytes());
            }
        });
        kafkaDS.addSink(kafkaSink);
        env.execute();
    }
}
