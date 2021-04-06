package com.joker.gmall.service;/*
 *项目名: gmall2021-parent
 *文件名: ProvinceStatsService
 *创建者: Joker
 *创建时间:2021/4/6 19:57
 *描述:

 */

import com.joker.gmall.bean.ProvinceStats;

import java.util.List;

public interface ProvinceStatsService {
    List<ProvinceStats> selectProvinceStats(int date);
}
