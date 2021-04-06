package com.joker.gmall.realtime.bean;/*
 *项目名: gmall2021-parent
 *文件名: TransientSink
 *创建者: Joker
 *创建时间:2021/4/6 10:53
 *描述:

 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {

}
