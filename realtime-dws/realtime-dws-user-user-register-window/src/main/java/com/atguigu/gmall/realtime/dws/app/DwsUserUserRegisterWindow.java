package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;

/**
 * 创建者：gml
 * 创建日期：2024-07-16
 * 功能描述：用户域用户注册各窗口汇总表
 * 110_DWS_用户注册需求实现	https://www.bilibili.com/video/BV1dv421y7eu?p=110&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(10025, 4, "DwsUserUserRegisterWindow", Constant.TOPIC_DWD_USER_REGISTER);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) throws IOException {
        // 核心业务逻辑处理
        // 1、读取DWD主题数据
        // 2、数据清洗过滤
        // 3、转换数据结构为 JavaBean
        SingleOutputStreamOperator<UserRegisterBean> beanStream = etl(stream);

        // 4、添加水位线
        SingleOutputStreamOperator<UserRegisterBean> withWaterMarkStream = withWaterMark(beanStream);

        // 5、分组开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> reduceStream = windowAndAgg(withWaterMarkStream);

        // 6、写入到doris
        reduceStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_REGISTER_WINSOW));
    }

    public SingleOutputStreamOperator<UserRegisterBean> windowAndAgg(SingleOutputStreamOperator<UserRegisterBean> withWaterMarkStream) {
        return withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<UserRegisterBean> elements, Collector<UserRegisterBean> out) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (UserRegisterBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            out.collect(element);
                        }
                    }
                });
    }

    public SingleOutputStreamOperator<UserRegisterBean> withWaterMark(SingleOutputStreamOperator<UserRegisterBean> beanStream) {
        return beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                return DateFormatUtil.dateTImeToTs(element.getCreateTime());
            }
        }));
    }

    public SingleOutputStreamOperator<UserRegisterBean> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String value, Collector<UserRegisterBean> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String createTime = jsonObject.getString("create_time");
                    String id = jsonObject.getString("id");
                    if (createTime != null && id != null) {
                        out.collect(new UserRegisterBean("", "", "", 1L, createTime));
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉数据" + value);
                }
            }
        });
    }
}
