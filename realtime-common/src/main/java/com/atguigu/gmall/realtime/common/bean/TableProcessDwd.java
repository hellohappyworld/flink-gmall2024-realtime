package com.atguigu.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 创建者：gml
 * 创建日期：2024-05-10
 * 功能描述：事实表动态分流：字典表结构
 */
//@Data
//@NoArgsConstructor
//@AllArgsConstructor
public class TableProcessDwd {
    //来源表名
    String sourceTable;

    //来源类型
    String sourceType;

    //目标表名
    String sinkTable;

    //输出字段
    String sinkColimns;

    //配置表操作类型
    String op;

    public TableProcessDwd(String sourceTable, String sourceType, String sinkTable, String sinkColimns, String op) {
        this.sourceTable = sourceTable;
        this.sourceType = sourceType;
        this.sinkTable = sinkTable;
        this.sinkColimns = sinkColimns;
        this.op = op;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColimns() {
        return sinkColimns;
    }

    public void setSinkColimns(String sinkColimns) {
        this.sinkColimns = sinkColimns;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }
}
