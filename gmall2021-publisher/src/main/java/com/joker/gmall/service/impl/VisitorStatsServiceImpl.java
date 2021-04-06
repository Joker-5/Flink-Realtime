package com.joker.gmall.service.impl;/*
 *项目名: gmall2021-parent
 *文件名: VisitorStatsServiceImpl
 *创建者: Joker
 *创建时间:2021/4/6 20:10
 *描述:

 */

import com.joker.gmall.bean.VisitorStats;
import com.joker.gmall.mapper.VisitorStatsMapper;
import com.joker.gmall.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {
    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHour(int date) {
        return visitorStatsMapper.selectVisitorStatsByHour(date);
    }

    @Override
    public Long getPv(int date) {
        return visitorStatsMapper.selectPv(date);
    }

    @Override
    public Long getUv(int date) {
        return visitorStatsMapper.selectUv(date);
    }
}
