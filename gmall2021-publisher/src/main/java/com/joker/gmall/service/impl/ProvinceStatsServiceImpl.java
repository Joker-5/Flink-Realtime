package com.joker.gmall.service.impl;/*
 *项目名: gmall2021-parent
 *文件名: ProvinceStatsServiceImpl
 *创建者: Joker
 *创建时间:2021/4/6 19:57
 *描述:

 */

import com.joker.gmall.bean.ProvinceStats;
import com.joker.gmall.mapper.ProvinceStatsMapper;
import com.joker.gmall.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> selectProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
