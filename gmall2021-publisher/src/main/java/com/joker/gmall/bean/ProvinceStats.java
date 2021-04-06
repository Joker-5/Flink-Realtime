package com.joker.gmall.bean;/*
 *项目名: gmall2021-parent
 *文件名: ProvinceStats
 *创建者: Joker
 *创建时间:2021/4/6 19:55
 *描述:

 */

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProvinceStats {
    private String stt;
    private String edt;
    private String province_id;
    private String province_name;
    private BigDecimal order_amount;
    private String ts;
}
