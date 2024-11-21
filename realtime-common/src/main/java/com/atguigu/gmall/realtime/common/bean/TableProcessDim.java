package com.atguigu.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 创建者：gml
 * 创建日期：2024-04-08
 * 功能描述：java Bean，用于映射flink cdc读取的mysql数据
 * https://www.bilibili.com/video/BV1dv421y7eu?p=26&vd_source=b6440733352819cc788f24606ec23fa3
 */
//@Data
//@NoArgsConstructor
//@AllArgsConstructor
public class TableProcessDim {
    //来源表名
    String sourceTable;

    //目标表名
    String sinkTable;

    //输出字段
    String sinkColumns;

    //数据到 hbase 的数据
    String sinkFamily;

    //sink到hbase的时候的主键信息
    String sinkRowKey;

    //配置表操作类型
    String op;

    public TableProcessDim() {
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public String getSinkFamily() {
        return sinkFamily;
    }

    public void setSinkFamily(String sinkFamily) {
        this.sinkFamily = sinkFamily;
    }

    public String getSinkRowKey() {
        return sinkRowKey;
    }

    public void setSinkRowKey(String sinkRowKey) {
        this.sinkRowKey = sinkRowKey;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    @Override
    public String toString() {
        return "TableProcessDim{" +
                "sourceTable='" + sourceTable + '\'' +
                ", sinkTable='" + sinkTable + '\'' +
                ", sinkColumns='" + sinkColumns + '\'' +
                ", sinkFamily='" + sinkFamily + '\'' +
                ", sinkRowKey='" + sinkRowKey + '\'' +
                ", op='" + op + '\'' +
                '}';
    }
}
