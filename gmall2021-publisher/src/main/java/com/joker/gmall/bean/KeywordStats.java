package com.joker.gmall.bean;/*
 *项目名: gmall2021-parent
 *文件名: KeywordStats
 *创建者: Joker
 *创建时间:2021/4/6 20:15
 *描述:

 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String stt;
    private String edt;
    private String keyword;
    private Long ct;
    private String ts;
}
