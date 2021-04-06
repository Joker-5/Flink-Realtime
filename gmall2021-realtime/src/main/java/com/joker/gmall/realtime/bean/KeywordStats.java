package com.joker.gmall.realtime.bean;/*
 *项目名: gmall2021-parent
 *文件名: KeywordStats
 *创建者: Joker
 *创建时间:2021/4/6 12:36
 *描述:
    关键词统计实体类
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String keyword;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;
}
