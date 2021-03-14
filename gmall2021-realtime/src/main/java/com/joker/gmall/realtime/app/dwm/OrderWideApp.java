package com.joker.gmall.realtime.app.dwm;/*
 *项目名: gmall2021-parent
 *文件名: OrderWideApp
 *创建者: Joker
 *创建时间:2021/3/14 11:15
 *描述:
    处理订单和订单明细数据形成订单宽表
 */

import com.alibaba.fastjson.JSON;
import com.joker.gmall.realtime.bean.OrderDetail;
import com.joker.gmall.realtime.bean.OrderInfo;
import com.joker.gmall.realtime.bean.OrderWide;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.types.OrderedInt8;
import org.codehaus.jackson.schema.JsonSchema;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(KafkaConstant.KAFKA_PARTITION);
        //TODO 2.从kafka读取数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        DataStreamSource<String> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));
        //TODO 3.订单，订单详情事实表双流join
        //数据结构转换便于进一步操作
        SingleOutputStreamOperator<OrderInfo> orderInfoMapDS = orderInfoDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //时分秒可能还需要设置,之后看运行结果进行修改 -> yyyy-MM-dd HH:mm:ss
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        //添加属性
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );
        SingleOutputStreamOperator<OrderDetail> orderDetailMapDS = orderDetailDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );
//        orderInfoDS.print("info---->");
//        orderDetailDS.print("detail---->");
        //时间窗口&水位线设置
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWaterMarkDS = orderInfoMapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<OrderInfo>) (orderInfo, recordTimestamp) -> orderInfo.getCreate_ts())

        );
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMarkDS = orderDetailMapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<OrderDetail>) (orderDetail, recordTimestamp) -> orderDetail.getCreate_ts())
        );
        //关联key设置
        KeyedStream<OrderInfo, Long> orderInfoLongKeyedStream = orderInfoWithWaterMarkDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailLongKeyedStream = orderDetailWithWaterMarkDS.keyBy(OrderDetail::getOrder_id);
        //intervalJoin双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoLongKeyedStream
                .intervalJoin(orderDetailLongKeyedStream)
                .between(Time.milliseconds(-5), Time.milliseconds(+5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
//        orderWideDS.print("orderWide---->");

        env.execute();
    }
}
