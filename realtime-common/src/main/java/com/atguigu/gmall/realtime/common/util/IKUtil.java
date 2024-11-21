package com.atguigu.gmall.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 创建者：gml
 * 创建日期：2024-06-19
 * 功能描述：IK分词器
 * https://www.bilibili.com/video/BV1dv421y7eu/?p=91&vd_source=b6440733352819cc788f24606ec23fa3
 * 93_DWS_自定义UDTF函数方法介绍	https://www.bilibili.com/video/BV1dv421y7eu?p=93&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class IKUtil {
    public static List<String> IKSplit(String keywords) {
        StringReader stringReader = new StringReader(keywords);
        //true代表智能模式，能够对产出的词进行去重
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        ArrayList<String> result = new ArrayList<>();
        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                result.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) throws IOException {

    }
}
