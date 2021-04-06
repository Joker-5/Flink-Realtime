package com.joker.gmall.service;/*
 *项目名: gmall2021-parent
 *文件名: KeywordStatsService
 *创建者: Joker
 *创建时间:2021/4/6 20:17
 *描述:

 */

import com.joker.gmall.bean.KeywordStats;

import java.util.List;

public interface KeywordStatsService {
    List<KeywordStats> getKeywordStats(int date, int limit);
}
