package com.joker.gmall.realtime.bean;/*
 *项目名: gmall2021-parent
 *文件名: PaymentInfo
 *创建者: Joker
 *创建时间:2021/4/5 16:09
 *描述:
支付信息实体类
 */

import lombok.Data;

import java.math.BigDecimal;

@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
