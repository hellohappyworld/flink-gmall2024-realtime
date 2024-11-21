package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.common.util.IKUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * 创建者：gml
 * 创建日期：2024-06-25
 * 功能描述：UDTF函数
 * 93_DWS_自定义UDTF函数方法介绍	https://www.bilibili.com/video/BV1dv421y7eu?p=93&vd_source=b6440733352819cc788f24606ec23fa3
 */
@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String keywords) {
        List<String> stringList = IKUtil.IKSplit(keywords);
        for (String s : stringList) {
            collect(Row.of(s));
        }
    }
}
