package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 创建者：gml
 * 创建日期：2024-07-12
 * 功能描述：将驼峰转换为蛇形
 * 100_DWS_流量域各粒度用户访问数据写出到doris  https://www.bilibili.com/video/BV1dv421y7eu?p=100&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T value) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSONObject.toJSONString(value, config);
    }
}
