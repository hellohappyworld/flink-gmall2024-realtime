package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 创建者：gml
 * 创建日期：2024-07-15
 * 功能描述：用户域用户登录各窗口汇总表
 * 107_DWS_用户登录统计判断独立用户和回流用户  https://www.bilibili.com/video/BV1dv421y7eu?p=107&vd_source=b6440733352819cc788f24606ec23fa3
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserLoginBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 当天日期
    String curDate;

    // 回流用户数
    Long backCt;

    // 独立用户数
    Long uuCt;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
