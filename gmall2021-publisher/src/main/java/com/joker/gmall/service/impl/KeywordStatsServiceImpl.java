package com.joker.gmall.service.impl;/*
 *项目名: gmall2021-parent
 *文件名: KeywordStatsServiceImpl
 *创建者: Joker
 *创建时间:2021/4/6 20:17
 *描述:

 */

import com.joker.gmall.bean.KeywordStats;
import com.joker.gmall.mapper.KeywordStatsMapper;
import com.joker.gmall.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date, limit);
    }
}
