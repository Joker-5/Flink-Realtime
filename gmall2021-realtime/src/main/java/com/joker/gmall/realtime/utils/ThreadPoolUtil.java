package com.joker.gmall.realtime.utils;/*
 *项目名: gmall2021-parent
 *文件名: ThreadPoolUtil
 *创建者: Joker
 *创建时间:2021/3/14 16:41
 *描述:

 */

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool;

    public static ThreadPoolExecutor getInstance() {
        //单例饿汉模式
        if (pool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {
                    pool = new ThreadPoolExecutor(4
                            , 20
                            , 300
                            , TimeUnit.SECONDS
                            , new LinkedBlockingDeque<>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return pool;
    }
}
