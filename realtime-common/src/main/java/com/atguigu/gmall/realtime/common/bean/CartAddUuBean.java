package com.atguigu.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 创建者：gml
 * 创建日期：2024-07-16
 * 功能描述：交易域加购各窗口汇总表
 * 111_DWS_用户加购需求分析和结构搭建	https://www.bilibili.com/video/BV1dv421y7eu?p=111&vd_source=b6440733352819cc788f24606ec23fa3
 */
@Data
@AllArgsConstructor
@Builder
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
