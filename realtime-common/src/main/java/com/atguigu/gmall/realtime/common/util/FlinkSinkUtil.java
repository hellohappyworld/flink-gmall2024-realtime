package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Random;

/**
 * 创建者：gml
 * 创建日期：2024-04-17
 * 功能描述：写出工具类
 * DWD_日志拆分业务实现 https://www.bilibili.com/video/BV1dv421y7eu?p=47&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_base_db数据写出 https://www.bilibili.com/video/BV1dv421y7eu?p=74&vd_source=b6440733352819cc788f24606ec23fa3
 * 100_DWS_流量域各粒度用户访问数据写出到doris  https://www.bilibili.com/video/BV1dv421y7eu?p=100&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)//设置精确一致性
                .setTransactionalIdPrefix("atguigu-" + topicName + System.currentTimeMillis())//设置事务ID
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")//事务超时时间：15min
                .build();
    }

    public static KafkaSink<JSONObject> getKafkaSinkWithTopicName() {
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaSinkContext context, Long timestamp) {
                        String topicName = element.getString("sink_table");
                        element.remove("sink_table");
                        return new ProducerRecord<byte[], byte[]>(topicName, Bytes.toBytes(element.toString()));
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)//设置精确一致性
                .setTransactionalIdPrefix("atguigu-" + "base_db" + System.currentTimeMillis())//设置事务ID
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")//事务超时时间：15min
                .build();
    }

    //Doris写出链接器
    public static DorisSink<String> getDorisSink(String tableName) {
        Properties properties = new Properties();
        // 上游是json写入时需要开启的配置
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .setLabelPrefix("label-doris" + System.currentTimeMillis())//数据流标签前缀
                                .setDeletable(false)
                                .setStreamLoadProp(properties)
                                .build()
                ).setSerializer(new SimpleStringSerializer())//序列化String
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.FENODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + tableName)
                        .setUsername(Constant.DORIS_USERNAME)
                        .setUsername(Constant.DORIS_PASSWORD)
                        .build())
                .build();

    }


}
