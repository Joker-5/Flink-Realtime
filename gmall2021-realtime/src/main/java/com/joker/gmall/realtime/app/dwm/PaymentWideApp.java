package com.joker.gmall.realtime.app.dwm;/*
 *项目名: gmall2021-parent
 *文件名: PaymentWideApp
 *创建者: Joker
 *创建时间:2021/4/5 16:11
 *描述:

 */

import com.alibaba.fastjson.JSON;
import com.joker.gmall.realtime.bean.OrderWide;
import com.joker.gmall.realtime.bean.PaymentInfo;
import com.joker.gmall.realtime.bean.PaymentWide;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.DateTimeUtil;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.utils.Java;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(KafkaConstant.KAFKA_PARTITION);

        //TODO 1.接收数据流
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        //封装 Kafka 消费者 读取支付流数据
        FlinkKafkaConsumer<String> paymentInfoSource =
                MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStream<String> paymentInfoJsonDS = env.addSource(paymentInfoSource);
        //对读取的支付数据进行转换
        DataStream<PaymentInfo> paymentInfoDStream =
                paymentInfoJsonDS.map(jsonString -> JSON.parseObject(jsonString,
                        PaymentInfo.class));
        //封装 Kafka 消费者 读取订单宽表流数据
        FlinkKafkaConsumer<String> orderWideSource =
                MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStream<String> orderWideJsonDS = env.addSource(orderWideSource);
        //对读取的订单宽表数据进行转换
        DataStream<OrderWide> orderWideDS =
                orderWideJsonDS.map(jsonString -> JSON.parseObject(jsonString,
                        OrderWide.class));
        //TODO 2.设置水位线
        SingleOutputStreamOperator<PaymentInfo> paymentInfoEventTimeDS =
                paymentInfoDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (paymentInfo, ts) -> DateTimeUtil.toTs(paymentInfo.getCallback_time())));

        SingleOutputStreamOperator<OrderWide> orderInfoWithEventTimeDS =
                orderWideDS.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (orderWide, ts) -> DateTimeUtil.toTs(orderWide.getCreate_time())
                                )
                );
        //设置分区键
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream =
                paymentInfoEventTimeDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream =
                orderInfoWithEventTimeDS.keyBy(OrderWide::getOrder_id);
        //TODO 3.关联数据(双流join)
        SingleOutputStreamOperator<PaymentWide> paymentWideDS =
                paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream).
                        between(Time.seconds(-1800), Time.seconds(0)).
                        process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo,
                                                       OrderWide orderWide,
                                                       Context ctx, Collector<PaymentWide> out) {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }).uid("payment_wide_join");
        SingleOutputStreamOperator<String> paymentWideStringDS =
                paymentWideDS.map(JSON::toJSONString);
        paymentWideStringDS.print("----->");
        //TODO 4.回写数据到kafka
        paymentWideStringDS.addSink(
                MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));
        env.execute();
    }
}
