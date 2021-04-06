package com.joker.gmall.mapper;/*
 *项目名: gmall2021-parent
 *文件名: KeywordStatsMapper
 *创建者: Joker
 *创建时间:2021/4/6 20:16
 *描述:

 */

import com.joker.gmall.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface KeywordStatsMapper {
    @Select("select keyword," +
            "sum(keyword_stats_2021.ct * " +
            "multiIf(source='SEARCH',10,source='ORDER',3,source='CART',2,source='CLICK',1,0)) ct"
            +
            " from keyword_stats_2021 where toYYYYMMDD(stt)=#{date} group by keyword " +
            "order by sum(keyword_stats_2021.ct) desc limit #{limit} ")
    List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int
            limit);
}
