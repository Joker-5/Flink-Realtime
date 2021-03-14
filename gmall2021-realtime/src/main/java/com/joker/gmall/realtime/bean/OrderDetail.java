package com.joker.gmall.realtime.bean;/*
 *项目名: gmall2021-parent
 *文件名: OrderDetail
 *创建者: Joker
 *创建时间:2021/3/14 11:14
 *描述:
    订单实体类
 */

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderDetail {
    Long id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}
