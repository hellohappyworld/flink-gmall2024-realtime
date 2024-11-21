package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.time.Duration;

/**
 * 创建者：gml
 * 创建日期：2024-04-11
 * 功能描述：流量域未经加工的事务事实表（日志分流）
 * https://www.bilibili.com/video/BV1dv421y7eu?p=41&vd_source=b6440733352819cc788f24606ec23fa3
 * https://www.bilibili.com/video/BV1dv421y7eu?p=43&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_日志拆分需求新旧访客修改代码实现：https://www.bilibili.com/video/BV1dv421y7eu?p=44&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_日志拆分需求新旧访客修复代码测试 https://www.bilibili.com/video/BV1dv421y7eu?p=45&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_日志拆分需求完成拆分 https://www.bilibili.com/video/BV1dv421y7eu?p=46&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_日志拆分业务实现 https://www.bilibili.com/video/BV1dv421y7eu?p=47&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务处理
        //1、进行ETL操作，过滤不完整的数据，使用flatmap操作同时实现：过滤、转化
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        //2、新老访客状态标记修复（创建键控状态，一般需要注册水位线来保证数据的有序性，注册3秒的水位线）
        /**
         * 新老访客状态标记修复思路：
         运用 Flink 状态编程，为每个 mid 维护一个键控状态，记录首次访问日期。
         一、如果 is new 的值为 1
         1、如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts对应的日期更新到状态中，不对 is new 字段做修改;
         2、如果键控状态不为null,且首次访问日期不是当日,说明访问的是老访客,将is new字段置为 0;
         3、如果键控状态不为null，且首次访问日期是当日，说明访问的是新访客，不做操作
         二、如果 is new 的值为 0
         1、如果键控状态为nul,说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日;
         2、如果键控状态不为null，说明程序已经维护了首次访问日期，不做操作。
         */
        KeyedStream<JSONObject, String> keyedStream = keyByWithWaterMark(jsonObjStream);
        SingleOutputStreamOperator<JSONObject> isNewFixStream = null;
        try {
            isNewFixStream = isNewFix(keyedStream);
        } catch (IOException e) {
        }

        //3、拆分不同类型的用户行为日志
        //启动日志：启动信息、报错信息
        //页面日志：页面信息、曝光信息、动作信息、报错信息
        //定义OutputTag时，有一个经典的bug，不能设定数据类型，要么加入花括号，要么使用类模板形式
//        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<String>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, startTag, errorTag, displayTag, actionTag);

        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errorTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    public SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> startTag, OutputTag<String> errorTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //核心逻辑，根据数据的不同 拆分到不同的侧输出流中
                JSONObject err = value.getJSONObject("err");
                if (err != null) {
                    //当前存在报错信息
                    ctx.output(errorTag, err.toJSONString());
                    value.remove("err");
                }

                JSONObject page = value.getJSONObject("page");
                JSONObject start = value.getJSONObject("start");
                JSONObject common = value.getJSONObject("common");
                JSONObject ts = value.getJSONObject("ts");
                if (start != null) {
                    //当前是启动日志 注意：输出的是value完整的日志信息
                    ctx.output(startTag, value.toJSONString());
                } else if (page != null) {
                    //当前为页面日志
                    //拆分曝光日志
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        //对JSONArray不要使用foreach循环，没有返回值类型，不好用，建议使用fori循环
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            ctx.output(displayTag, display.toJSONString());
                        }
                        value.remove("displays");
                    }

                    //拆分动作日志
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            ctx.output(actionTag, action.toJSONString());
                        }
                        value.remove("actions");
                    }

                    // value进行上述remove后，只剩下page信息，作为主流输出
                    out.collect(value.toJSONString());

                } else {
                    //留空 不错操作
                }

            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keyedStream) throws java.io.IOException {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建状态
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                //1、获取当前数据的is_new字段
                JSONObject common = value.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    //判断当前状态情况
                    if (firstLoginDt != null && !firstLoginDt.equals(curDt)) {
                        //如果状态不为空，日期也不是今天，说明当前数据错误，不是新访客，伪装新访客
                        common.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        //状态为空
                        firstLoginDtState.update(curDt);
                    } else {
                        //不操作
                        //当前数据是同一天新访客重复登录
                    }
                } else {
                    //is_new为0
                    if (firstLoginDt == null) {
                        //flink实时数仓里面没有记录过这个访客，需要补充该老访客的信息
                        //把访客首次登录的日期补充一个值：今天以前的任意一天都可以
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        //留空
                        //正常情况，不需要修复
                    }
                }
                out.collect(common);
            }
        });
    }

    public KeyedStream<JSONObject, String> keyByWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        //注意flink中的水位线统一用毫秒，如果ts不是毫秒，需要改为毫秒
                        return element.getLong("ts");
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {
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
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (page != null || start != null) {
                        //当mid或者ts为null时，接下来的注册水位线和keyBy操作会报错，报错信息如：
                        //org.apache.flink.streaming.runtime.tasks,ExceptionInchainedoperatorException: Could not forward element to next operator
                        //Caused by: java.lang.NullPointerException: Assigned key must not be null!
                        if (common != null && common.getString("mid") != null && ts != null) {
                            out.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
