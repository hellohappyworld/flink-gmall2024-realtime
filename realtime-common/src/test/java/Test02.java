import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * 创建者：gml
 * 创建日期：2024-04-01
 * 功能描述：
 */
public class Test02 {
    public static void main(String[] args) {
        //1、构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        //2、添加参数
//        //创建状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        //开启 checkpoint
//        env.enableCheckpointing(5000);
//        //设置 checkpoint 模式：精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //checkpoint 存储
//        env.getCheckpointConfig().setCheckpointStorage("");
//        //checkpoint 并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        //checkpoint 之间的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        //checkpoint 的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        //job 取消时 checkpoint 保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);


        //3、读取数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSOL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("gmall2023_config")//库名
                .tableList("gmall2023_config.table_process_dim")//表名，需要带库名
                .deserializer(new JsonDebeziumDeserializationSchema())//反序列化为json格式
                .startupOptions(StartupOptions.initial())//推荐使用初始化读取方式，一开始会将整张表读一遍，后续再对变更数据进行抓取
                .build();


        //注意：使用flinkCDC读取mysql的beanlog时必须设置为一个分区
        DataStreamSource<String> kafkaSource = env.fromSource(mySqlSource, WatermarkStrategy.<String>noWatermarks(),
                "mysql_source").setParallelism(1);

        //4、对数据源进行处理
        kafkaSource.print();

        //5、执行环境
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
