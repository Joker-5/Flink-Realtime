package com.joker.gmall.realtime.app.dwm;/*
 *项目名: gmall2021-parent
 *文件名: OrderWideApp
 *创建者: Joker
 *创建时间:2021/3/14 11:15
 *描述:
    处理订单和订单明细数据形成订单宽表
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.joker.gmall.realtime.app.func.DimAsyncFunction;
import com.joker.gmall.realtime.bean.OrderDetail;
import com.joker.gmall.realtime.bean.OrderInfo;
import com.joker.gmall.realtime.bean.OrderWide;
import com.joker.gmall.realtime.common.constant.KafkaConstant;
import com.joker.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.types.OrderedInt8;
import org.codehaus.jackson.schema.JsonSchema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

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
        orderWideDS.print("orderWide---->");
        //TODO 4.关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws ParseException {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                        Date date = sdf.parse(birthday);
                        long curTs = System.currentTimeMillis();
                        long betweenMs = curTs - date.getTime();
                        long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = (int) ageLong;
                        orderWide.setUser_age(age);
                        orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
                    }
                },
                60, TimeUnit.SECONDS);
        //TODO 5.关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setProvince_name(jsonObject.getString("NAME"));
                        orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                    }
                }
                , 60, TimeUnit.SECONDS);
//        orderWideWithProvinceDS.print("province>==");
        //TODO 6.关联sku维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithSkuDS.print("sku->");
        //TODO 7.关联spu商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithSpuDS.print("spu->");
        //TODO 8.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) {
                                orderWide.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //TODO 9.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithTmDS.print("tradeMark->");
        //TODO 10.回写到kafka订单宽表
        orderWideWithTmDS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));
        env.execute();
    }
}
