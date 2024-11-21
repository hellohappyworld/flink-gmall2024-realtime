package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 创建者：gml
 * 创建日期：2024-06-26
 * 功能描述：流量域各粒度(版本、渠道、地区、访客类别)页面浏览各窗口汇总表需求
 * 98_DWS_流量域各粒度用户页面访问窗口聚合得到独立访客和会话数	https://www.bilibili.com/video/BV1dv421y7eu/?p=98&spm_id_from=pageDriver&vd_source=b6440733352819cc788f24606ec23fa3
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TrafficPageViewBean {
    //窗口起始时间
    private String stt;
    //窗口结束时间
    private String ett;
    //当天日期
    private String cur_date;

    //app 版本号
    private String vc;
    //渠道
    private String ch;
    //地区
    private String ar;
    //新老访客状态标记
    private String isNew;

    //独立访客数
    private Long uvCt;
    //会话数
    private Long svCt;
    //页面浏览数
    private Long pvCt;
    //累计访问时长
    private Long durSum;

    @JSONField(serialize = false)
    private Long ts;
    @JSONField(serialize = false)
    private String sid;
}
