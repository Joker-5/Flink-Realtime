package com.joker.gmall.mapper;/*
 *项目名: gmall2021-parent
 *文件名: ProvinceStatsMapper
 *创建者: Joker
 *创建时间:2021/4/6 19:56
 *描述:

 */

import com.joker.gmall.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ProvinceStatsMapper {
    //按地区查询交易额
    @Select("select province_name,sum(order_amount) order_amount " +
            "from province_stats_2021 where toYYYYMMDD(stt)=#{date} " +
            "group by province_id ,province_name ")
    List<ProvinceStats> selectProvinceStats(int date);
}
