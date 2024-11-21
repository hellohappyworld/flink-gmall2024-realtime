package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 创建者：gml
 * 创建日期：2024-04-29
 * 功能描述：评论事务事实表
 * DWD_baseSQLApp封装读取topicdb方法 https://www.bilibili.com/video/BV1dv421y7eu?p=54&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_baseSQLAPP封装实现及使用方法 https://www.bilibili.com/video/BV1dv421y7eu?p=55&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_评论事实表完成HBase的lookup_join https://www.bilibili.com/video/BV1dv421y7eu?p=56&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_评论事实表完成数据写出 https://www.bilibili.com/video/BV1dv421y7eu?p=57&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, "dwd-interaction-comment-info");
    }


    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //核心逻辑
        //1、读取topic_db
        createTopicDb(groupId, tableEnv);

        //2、读取base_dic
        createBaseDic(tableEnv);

        //3、清洗topic_db，筛选出评论信息表新增的数据
        filterCommentInfo(tableEnv);

        //4、使用 lookup join 完成维度退化
        Table joinTable = lookUpJoin(tableEnv);

        //5、创建 kafka sink 对应的表格
        createKafkaSink(tableEnv);

        //6、写出到对应的kafka中
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INF0).execute();

    }

    public void createKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INF0 + "(id STRING,\n" +
                "user_id STRING,\n" +
                "nick_name STRING,\n" +
                "sku_id STRING,\n" +
                "spu_id STRING,\n" +
                "order_id STRING,\n" +
                "appraise_code STRING,\n" +
                "appraise_name STRING,\n" +
                "comment_txt STRING,\n" +
                "create_time STRING,\n" +
                "operate_time STRING" +
                ")"
                + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INF0));
    }

    public Table lookUpJoin(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                    " id,\n" +
                    " user_id,\n" +
                    " nick_name,\n" +
                    " sku_id,\n" +
                    " spu_id,\n" +
                    " order_id,\n" +
                    " appraise appraise_code,\n" +
                    " info.dic_name appraise_name,\n" +
                    " comment_txt,\n" +
                    " create_time,\n" +
                    " operate_time\n" +
                    "from comment_info c\n" +
                    "join base_dic FOR SYSTEM_TIME AS OF c.proc_time as b\n" +
                    "on c.appraise=b.rowkey");
    }

    public void filterCommentInfo(StreamTableEnvironment tableEnv) {
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                " `data`.['id'] id,\n" +
                " `data`.['user_id'] user_id,\n" +
                " `data`.['nick_name'] nick_name,\n" +
                " `data`.['head_img'] head_img,\n" +
                " `data`.['sku_id'] sku_id,\n" +
                " `data`.['spu_id'] spu_id,\n" +
                " `data`.['order_id'] order_id,\n" +
                " `data`.['appraise'] appraise,\n" +
                " `data`.['comment_txt'] comment_txt,\n" +
                " `data`.['create_time'] create_time,\n" +
                " `data`.['operate_time'] operate_time,\n" +
                " proc_time\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='comment_info'\n" +
                "and `type`='insert'");
        //创建视图
        tableEnv.createTemporaryView("comment_info", commentInfo);
    }
}
