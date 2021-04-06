package com.joker.gmall.realtime.utils;/*
 *项目名: gmall2021-parent
 *文件名: MyKafkaUtil
 *创建者: Joker
 *创建时间:2021/3/7 13:10
 *描述:
    kafka工具类
 */

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    private static final String KAFKA_SERVER = "hadoop202:9092,hadoop203:9092,hadoop204:9092";
    private static final String DEFAULT_TOPIC = "DEFAULT_DATA";

    //封装kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
    }

    //封装kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(KAFKA_SERVER, topic, new SimpleStringSchema());
    }

    //封装kafka生产者,使其动态地向不同topic发送数据并序列化
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> serializationSchema) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        //15 min未更新状态则超时
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 1000 * 60 + "");
        return new FlinkKafkaProducer<>(DEFAULT_TOPIC, serializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //从kafka读取数据拼接ddl
    public static String getKafkaDDL(String topic, String groupId) {
        String ddl = "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset' ";
        return ddl;
    }
}
