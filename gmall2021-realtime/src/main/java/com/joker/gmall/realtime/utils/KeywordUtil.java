package com.joker.gmall.realtime.utils;/*
 *项目名: gmall2021-parent
 *文件名: KeywordUtil
 *创建者: Joker
 *创建时间:2021/4/6 12:23
 *描述:
    分词工具类
 */

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    //使用 IK 分词器对字符串进行分词
    public static List<String> analyze(String text) {
        StringReader sr = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(sr, true);
        Lexeme lex = null;
        List<String> keywordList = new ArrayList();
        while (true) {
            try {
                if ((lex = ik.next()) != null) {
                    String lexemeText = lex.getLexemeText();
                    keywordList.add(lexemeText);
                } else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return keywordList;
    }

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信 4G 手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));
    }
}
