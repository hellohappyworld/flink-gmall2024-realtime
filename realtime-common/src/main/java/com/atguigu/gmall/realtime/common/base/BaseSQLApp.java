package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * 创建者：gml
 * 创建日期：2024-04-29
 * 功能描述：Flink SQL基类
 * DWD_baseSQLApp封装读取topicdb方法 https://www.bilibili.com/video/BV1dv421y7eu?p=54&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_baseSQLAPP封装实现及使用方法 https://www.bilibili.com/video/BV1dv421y7eu?p=55&vd_source=b6440733352819cc788f24606ec23fa3
 */
public abstract class BaseSQLApp {
    public void start(int port, int parallelism, String ckAndGroupId) {
        //设置用户名，用于checkpoint到hdfs
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //获取流处理环境，并指定本地测试时启动WebUI所绑定的端口号
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        //1、构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        //2、添加参数
        //创建状态后端
        env.setStateBackend(new HashMapStateBackend());
        //开启 checkpoint
        env.enableCheckpointing(5000);
        //设置 checkpoint 模式：精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("");
        //checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        //创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3、读取topic_db数据
//        createTopicDb(ckAndGroupId, tableEnv);

        //4、对数据源进行处理：封装抽象处理方法
        handle(tableEnv, env, ckAndGroupId);
    }

    //读取topic_db数据
    public void createTopicDb(String ckAndGroupId, StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(SQLUtil.getKafkaTopicDb(ckAndGroupId));
    }

    public abstract void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId);

    //读取HBase中的base_dic字典表
    public void createBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " rowkey STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '" + Constant.HBASE_Z00KEEPER_QUORUM + "'\n" +
                ")");
    }

}
