package com.joker.gmall.realtime.utils;/*
 *项目名: gmall2021-parent
 *文件名: DateTimeUtil
 *创建者: Joker
 *创建时间:2021/4/5 16:40
 *描述:
    日期转换工具类
 */

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTimeUtil {
    public final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(),
                ZoneId.systemDefault());
        return formatter.format(localDateTime);
    }

    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formatter);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
