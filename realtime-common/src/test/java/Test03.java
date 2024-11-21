import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 创建者：gml
 * 创建日期：2024-04-17
 * 功能描述：
 * DWD_flinkSQL的使用架构 https://www.bilibili.com/video/BV1dv421y7eu/?p=48&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_flinkSQL读取不同的数据源数据 https://www.bilibili.com/video/BV1dv421y7eu?p=49&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_flinkSQL完成两种表格的join操作	https://www.bilibili.com/video/BV1dv421y7eu?p=50&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_flinkSQL的基础原理介绍 https://www.bilibili.com/video/BV1dv421y7eu?p=51&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_LookUp join使用场景介绍	https://www.bilibili.com/video/BV1dv421y7eu?p=52&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_LookUp join具体使用演示 https://www.bilibili.com/video/BV1dv421y7eu?p=53&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class Test03 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //状态的存活时间 ttl
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10L));


        //创建评论表
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` bigint,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "  proc_time as PROCTIME() \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'test03',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        //创建字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "  dic_code STRING,\n" +
                "  dic_name STRING,\n" +
                "  parent_code STRING,\n" +
                "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '000000'\n" +
                ")");

        //过滤出common_info的对应信息
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

        //创建视图，以便于进行子查询
        tableEnv.createTemporaryView("comment_info", commentInfo);

        //join
        tableEnv.executeSql("select\n" +
                " id,\n" +
                " user_id,\n" +
                " nick_name,\n" +
                " head_img,\n" +
                " sku_id,\n" +
                " spu_id,\n" +
                " order_id,\n" +
                " appraise appraise_code,\n" +
                " b.dic_name appraise_name,\n" +
                " comment_txt,\n" +
                " create_time,\n" +
                " operate_time\n" +
                "from comment_info c\n" +
                "join base_dic FOR SYSTEM_TIME AS OF c.proc_time as b\n" +
                "on c.appraise=b.dic_code");

    }
}
