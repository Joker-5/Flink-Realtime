package com.joker.gmall.mapper;/*
 *项目名: gmall2021-parent
 *文件名: ProductStatsMapper
 *创建者: Joker
 *创建时间:2021/4/6 13:07
 *描述:
    商品统计mapper
 */

import com.joker.gmall.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsMapper {
    //获取商品交易额
    @Select("select sum(order_amount) order_amount " +
            "from product_stats_2021 where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);

    //统计某天不同 SPU 商品交易额排名
    @Select("select spu_id,spu_name,sum(order_amount) order_amount," +
            "sum(order_ct) order_ct from product_stats_2021 " +
            "where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name " +
            "having order_amount>0 order by order_amount desc limit #{limit} ")
    List<ProductStats> getProductStatsGroupBySpu(@Param("date") int date, @Param("limit")
            int limit);

    //统计某天不同类别商品交易额排名
    @Select("select category3_id,category3_name,sum(order_amount) order_amount " +
            "from product_stats_2021 " +
            "where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name " +
            "having order_amount>0 order by order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsGroupByCategory3(@Param("date") int date,
                                                       @Param("limit") int limit);

    //统计某天不同品牌商品交易额排名
    @Select("select tm_id,tm_name,sum(order_amount) order_amount " +
            "from product_stats_2021 " +
            "where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name " +
            "having order_amount>0 order by order_amount desc limit #{limit} ")
    List<ProductStats> getProductStatsByTrademark(@Param("date") int date, @Param("limit")
            int limit);
}
