package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 创建者：yml
 * 创建日期：2024-07-15
 * 功能描述：流量域首页、详情页页面浏览各窗口汇总表
 * 102_DWS_首页详情页独立访客统计需求分析 https://www.bilibili.com/video/BV1dv421y7eu?p=102&vd_source=b6440733352819cc788f24606ec23fa3
 */
@Data
@AllArgsConstructor
@Builder
public class TrafficHomeDetailPageViewBean {
    //窗口起始时间
    String stt;
    //窗口结束时间
    String edt;
    //当天日期
    String curDate;
    //首页独立访客数
    Long homeUvCt;
    //商品详情页独立访客数
    Long goodDetailUvct;
    //时间戳
    @JSONField(serialize = false)
    Long ts;
}
