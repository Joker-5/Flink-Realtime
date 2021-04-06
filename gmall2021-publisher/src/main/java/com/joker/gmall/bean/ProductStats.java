package com.joker.gmall.bean;/*
 *项目名: gmall2021-parent
 *文件名: ProductStats
 *创建者: Joker
 *创建时间:2021/4/6 19:45
 *描述:
    商品交易额实体类
 */

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;


import lombok.Builder;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ProductStats {
    String stt;
    String edt;
    Long sku_id;
    String sku_name;
    BigDecimal sku_price;
    Long spu_id;
    String spu_name;
    Long tm_id;
    String tm_name;
    Long category3_id;
    String category3_name;
    @Builder.Default
    Long display_ct = 0L;
    @Builder.Default
    Long click_ct = 0L;
    @Builder.Default
    Long cart_ct = 0L;
    @Builder.Default
    Long order_sku_num = 0L;
    @Builder.Default
    BigDecimal order_amount = BigDecimal.ZERO;
    @Builder.Default
    Long order_ct = 0L;
    @Builder.Default
    BigDecimal payment_amount = BigDecimal.ZERO;
    @Builder.Default
    Long refund_ct = 0L;
    @Builder.Default
    BigDecimal refund_amount = BigDecimal.ZERO;
    @Builder.Default
    Long comment_ct = 0L;
    @Builder.Default
    Long good_comment_ct = 0L;
    Long ts;
}
