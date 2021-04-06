package com.joker.gmall.service;/*
 *项目名: gmall2021-parent
 *文件名: VisitorStatsService
 *创建者: Joker
 *创建时间:2021/4/6 20:09
 *描述:

 */

import com.joker.gmall.bean.VisitorStats;

import java.util.List;

public interface VisitorStatsService {
    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    List<VisitorStats> getVisitorStatsByHour(int date);

    Long getPv(int date);

    Long getUv(int date);
}
