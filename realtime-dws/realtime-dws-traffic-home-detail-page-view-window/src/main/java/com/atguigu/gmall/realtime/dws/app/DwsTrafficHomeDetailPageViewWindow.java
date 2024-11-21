package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;

/**
 * 创建者：gml
 * 创建日期：2024-07-15
 * 功能描述：流量域首页、详情页页面浏览各窗口汇总表
 * 102_DWS_首页详情页独立访客统计需求分析 https://www.bilibili.com/video/BV1dv421y7eu?p=102&vd_source=b6440733352819cc788f24606ec23fa3
 * 103_DWS_首页详情页独立访客判断代码实现 https://www.bilibili.com/video/BV1dv421y7eu?p=103&vd_source=b6440733352819cc788f24606ec23fa3
 * 104_DWS_首页详情页独立访客数据写出到doris https://www.bilibili.com/video/BV1dv421y7eu?p=104&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10023, 4, "dws-traffic-home-detail-page-view-window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) throws IOException {
        // 核心业务处理
        // 1、读取DWD层page主题

        // 2、清洗过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        //3、按照mid分组
        KeyedStream<JSONObject, String> keyedStream = getKeyedStream(jsonObjStream);

        //4、判断独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream = uvCountBean(keyedStream);

        //5、添加水位线
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream = withWaterMark(processBeanStream);

        //6、分组开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = windowAndAgg(withWaterMarkStream);

        //7、写出到Kafka
        reduceStream.map(new DorisMapFunction<TrafficHomeDetailPageViewBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
    }

    public SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream) {
        return withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        // 将度量值合并在一起
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvct(value1.getGoodDetailUvct() + value2.getGoodDetailUvct());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDate(window.getStart());
                        String edt = DateFormatUtil.tsToDate(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TrafficHomeDetailPageViewBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setCurDate(curDt);
                            out.collect(value);
                        }
                    }
                });
    }

    public SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMark(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream) {
        return processBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(
                new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficHomeDetailPageViewBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }
        ));
    }

    public SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvCountBean(KeyedStream<JSONObject, String> keyedStream) throws IOException {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

            ValueState<String> homeLastLoginState;
            ValueState<String> detailLastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeLastLoginDesc = new ValueStateDescriptor<String>("home_last_login", String.class);
                homeLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                homeLastLoginState = getRuntimeContext().getState(homeLastLoginDesc);

                ValueStateDescriptor<String> detailLastLoginDesc = new ValueStateDescriptor<String>("detail_last_login", String.class);
                detailLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                detailLastLoginState = getRuntimeContext().getState(detailLastLoginDesc);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                // 判断独立访客 -> 状态存储的日期和当前数据的日期
                String pageId = value.getJSONObject("page").getString("page_id");
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                //首页独立访客数
                Long homeUvCt = 0L;
                //商品详情页独立访客数
                Long goodDetailUvCt = 0L;
                if ("home".equals(pageId)) {
                    String homeLastLoginDt = homeLastLoginState.value();
                    if (homeLastLoginDt == null || !homeLastLoginDt.equals(curDt)) {
                        // 首页的独立访客
                        homeUvCt = 1L;
                        homeLastLoginState.update(curDt);
                    }
                } else {
                    // 商品详情页
                    String detailLastLoginDt = detailLastLoginState.value();
                    if (detailLastLoginDt == null || !detailLastLoginDt.equals(curDt)) {
                        goodDetailUvCt = 1L;
                        detailLastLoginState.update(curDt);
                    }
                }
                // 如果两个独立访客的度量值都为0，可以过滤掉，不需要往下游发送
                if (homeUvCt != 0 || goodDetailUvCt != 0) {
                    out.collect(TrafficHomeDetailPageViewBean.builder()
                            .homeUvCt(homeUvCt)
                            .goodDetailUvct(goodDetailUvCt)
                            .ts(ts)
                            .build()
                    );
                }


            }
        });
    }

    public KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(value);
                    JSONObject page = jsonObj.getJSONObject("page");
                    String pageId = page.getString("page_id");
                    String mid = jsonObj.getJSONObject("common").getString("mid");
                    if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                        if (mid != null) {
                            out.collect(jsonObj);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("过滤出脏数据" + value);
                }
            }
        });
    }
}
