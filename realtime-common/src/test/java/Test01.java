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
public class Test01 {
    public static void main(String[] args) {
        //1、构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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


        //3、读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("hadoop102:9092")//连接地址
                        .setTopics("topic_db")//topic地址
                        .setStartingOffsets(OffsetsInitializer.earliest())//从最早开始读取数据
                        .setGroupId("test01")//消费者组
                        .setValueOnlyDeserializer(new SimpleStringSchema())//序列化器
                        .build(), WatermarkStrategy.<String>noWatermarks(), "kafka_source");

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
