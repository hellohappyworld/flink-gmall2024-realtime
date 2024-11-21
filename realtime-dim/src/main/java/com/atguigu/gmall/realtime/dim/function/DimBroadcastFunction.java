package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

/**
 * 创建者：gml
 * 创建日期：2024-04-09
 * 功能描述：封装broadcastProcessFunction
 * https://www.bilibili.com/video/BV1dv421y7eu?p=35&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    public HashMap<String, TableProcessDim> hashMap;
    public MapStateDescriptor<String, TableProcessDim> broadcastState;

    //构造器传参
    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        /**
         * 预加载初始的维度表信息：当任务启动的时候，由于主流(kafka数据)比广播流(flinkCDC配置表数据)来的快，因此启动任务时会丢掉主流的数据，没法进行处理;
         * 由于open方法是任务启动时先执行，因此在此处通过jdbc方式预加载初始的维度表数据
         */
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gamll2023_config.table_process_dim", TableProcessDim.class, true);
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    /**
     * 处理广播流数据
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(TableProcessDim value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 读取广播状态
        BroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        // 将配置表信息作为一个维度表的标志 写到广播状态
        String op = value.getOp();
        if ("d".equals(op)) {
            //删除操作
            tableProcessState.remove(value.getSourceTable());
            //同步删除hashMap中初始化加载的配置表信息
            hashMap.remove(value.getSourceTable());
        } else {
            //增、改、查操作
            tableProcessState.put(value.getSourceTable(), value);
        }
    }

    /**
     * 处理主流数据
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        // 查询广播状态 判断当前的数据对应的表格是否存在于状态里面
        String tableName = value.getString("table");
        TableProcessDim tableProcessDim = tableProcessState.get(tableName);

        //为避免主流数据到的太早，造成状态为空，添加一层状态初始化判断
        if (tableProcessDim == null) {
            tableProcessDim = hashMap.get(tableName);
        }

        if (tableProcessDim != null) {
            // 状态不为空，说明当前一行数据是维度表数据
            out.collect(Tuple2.of(value, tableProcessDim));
        }
    }
}
