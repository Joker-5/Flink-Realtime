package com.joker.gmall.service;/*
 *项目名: gmall2021-parent
 *文件名: ProductStatsService
 *创建者: Joker
 *创建时间:2021/4/6 13:09
 *描述:

 */

import com.joker.gmall.bean.ProductStats;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsService {
    //获取某一天的总交易额
    BigDecimal getGMV(int date);

    //统计某天不同 SPU 商品交易额排名
    List<ProductStats> getProductStatsGroupBySpu(int date, int limit);

    //统计某天不同类别商品交易额排名
    List<ProductStats> getProductStatsGroupByCategory3(int date, int limit);

    //统计某天不同品牌商品交易额排名
    List<ProductStats> getProductStatsByTrademark(int date, int limit);
}
