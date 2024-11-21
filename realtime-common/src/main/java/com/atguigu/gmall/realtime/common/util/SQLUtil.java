package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * 创建者：gml
 * 创建日期：2024-04-29
 * 功能描述：SQL工具包
 * DWD_baseSQLApp封装读取topicdb方法 https://www.bilibili.com/video/BV1dv421y7eu?p=54&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_评论事实表完成数据写出 https://www.bilibili.com/video/BV1dv421y7eu?p=57&vd_source=b6440733352819cc788f24606ec23fa3
 * 96_DWS_关键词开窗统计数据写出到doris  https://www.bilibili.com/video/BV1dv421y7eu?p=96&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class SQLUtil {
    public static String getKafkaSourceSQL(String topicName, String groupId) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getKafkaTopicDb(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "  `ts` bigint,\n" +
                "  proc_time as PROCTIME(), \n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts*1000,3), \n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
                ") " + getKafkaSourceSQL(Constant.TOPIC_DB, groupId);
    }

    public static String getKafkaSinkSQL(String topicName) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    /**
     * 获取upsertkafka的连接，创建表格的语句最后一定要声明主键
     *
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSQL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");";
    }

    public static String getDorisSinkSQL(String tableName) {
        return "WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '" + Constant.FENODES + "',\n" +
                "      'table.identifier' = '" + Constant.DORIS_DATABASE + "." + tableName + "',\n" +
                "      'username' = '" + Constant.DORIS_USERNAME + "',\n" +
                "      'password' = '" + Constant.DORIS_PASSWORD + "',\n" +
                "      'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "'\n" +
                ")";
    }

}
