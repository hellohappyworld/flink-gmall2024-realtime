package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import com.atguigu.gmall.realtime.dim.function.DimBroadcastFunction;
import com.atguigu.gmall.realtime.dim.function.DimHBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * 创建者：gml
 * 创建日期：2024-04-01
 * 功能描述：基类的使用
 * 来源：https://www.bilibili.com/video/BV1dv421y7eu?p=27&vd_source=b6440733352819cc788f24606ec23fa3
 * https://www.bilibili.com/video/BV1dv421y7eu?p=26&vd_source=b6440733352819cc788f24606ec23fa3
 * https://www.bilibili.com/video/BV1dv421y7eu?p=33&vd_source=b6440733352819cc788f24606ec23fa3
 * https://www.bilibili.com/video/BV1dv421y7eu?p=37&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑：产出dim层维度表
        //1、对ods读取的原始数据进行数据清洗
        //flatMap 可以实现上述 过滤+转化
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        //2、使用flinkCDC读取监控配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        //注意：使用flinkCDC读取mysql的beanlog时必须设置为一个分区
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),
                "mysql_source").setParallelism(1);

        //3、在HBase中创建维度表，此处使用RichFlatMapFunction的目的是该rich方法具有生命周期
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHbaseTable(mysqlSource);

        //4、做成广播流
        //广播状态的key用于判断是否是维度表 value用于补充信息写出到hbase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createTableStream.broadcast(broadcastState);

        //5、连接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectionStream(jsonObjStream, broadcastState, broadcastStateStream);

        //6、筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = filterColumn(dimStream);

        //7、写出到HBase
        filterColumnStream.addSink(new DimHBaseSinkFunction());

    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumn(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDim dim = value.f1;

                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObj.getJSONObject("data");
                //将主流数据中的写出字段与配置表中的写出字段对齐
                data.keySet().removeIf(key -> !columns.contains(key));

                return value;
            }
        });
        return filterColumnStream;
    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectionStream(SingleOutputStreamOperator<JSONObject> jsonObjStream, MapStateDescriptor<String, TableProcessDim> broadcastState, BroadcastStream<TableProcessDim> broadcastStateStream) {
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = jsonObjStream.connect(broadcastStateStream);
        //此处并行度设置为1的目的是，便于在open方法中预加载配置表信息
        return connectStream.process(new DimBroadcastFunction(broadcastState)).setParallelism(1);
    }

    public SingleOutputStreamOperator<TableProcessDim> createHbaseTable(DataStreamSource<String> mysqlSource) {
        return mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取连接
                connection = HBaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                //关闭连接
                HBaseUtil.closeConnection(connection);
            }

            @Override
            public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
                //使用读取的配置表数据，到hbase中创建与之对应的表格
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        //删除操作
                        //类模板获取对象
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        //当配置表发送一个D类型的数据时，对应hbase需要删除一张维度表
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        //创建、读取操作
                        //类模板获取对象
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        createTable(dim);
                    } else {
                        //更改操作，先删除再创建
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    out.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                //切分出多个列族
                String[] split = sinkFamily.split(",");
                try {
                    HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            private void deleteTable(TableProcessDim dim) {
                try {
                    HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //此处是被封装的函数，封装函数的快捷键(Extract Method)：Ctrl+Alt+M
    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) &&
                            !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)
                            && data != null && data.size() != 0) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return jsonObjStream;
    }
}
