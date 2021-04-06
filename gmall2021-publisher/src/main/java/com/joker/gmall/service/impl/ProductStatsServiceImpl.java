package com.joker.gmall.service.impl;/*
 *项目名: gmall2021-parent
 *文件名: ProductStatsServiceImpl
 *创建者: Joker
 *创建时间:2021/4/6 13:09
 *描述:

 */

import com.joker.gmall.bean.ProductStats;
import com.joker.gmall.mapper.ProductStatsMapper;
import com.joker.gmall.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }

    @Override
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit) {
        return productStatsMapper.getProductStatsGroupBySpu(date, limit);
    }

    @Override
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsGroupByCategory3(date, limit);
    }

    @Override
    public List<ProductStats> getProductStatsByTrademark(int date, int limit) {
        return productStatsMapper.getProductStatsByTrademark(date, limit);
    }
}