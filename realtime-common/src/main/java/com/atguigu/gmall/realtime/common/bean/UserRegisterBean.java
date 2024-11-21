package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 创建者：gml
 * 创建日期：2024-07-16
 * 功能描述：用户域用户注册各窗口汇总表
 * 110_DWS_用户注册需求实现	https://www.bilibili.com/video/BV1dv421y7eu?p=110&vd_source=b6440733352819cc788f24606ec23fa3
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 注册用户数
    Long registerCt;

    @JSONField(serialize = false)
    String createTime;
}
