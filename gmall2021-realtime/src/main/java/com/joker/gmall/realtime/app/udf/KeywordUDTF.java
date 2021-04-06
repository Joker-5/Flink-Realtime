package com.joker.gmall.realtime.app.udf;/*
 *项目名: gmall2021-parent
 *文件名: KeywordUDTF
 *创建者: Joker
 *创建时间:2021/4/6 12:32
 *描述:
    自定义UDTF实现分词
 */

import com.joker.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String value) {
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
            Row row = new Row(1);
            row.setField(0, keyword);
            collect(row);
        }
    }
}
