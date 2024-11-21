package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 创建者：gml
 * 创建日期：2024-04-01
 * 功能描述：工具类
 */
public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupId, String topicName) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(
                        // SimpleStringSchema 无法反序列化null值的数据 会直接报错
                        //后续dwd层会向kafka中发送null值 不能使用SimpleStringSchema
                        //new SimpleStringSchema()
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null && message.length != 0) {
                                    return new String(message, StandardCharsets.UTF_8);
                                }
                                return "";
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                //基础类型信息
                                return BasicTypeInfo.STRING_TYPE_INFO;
                            }
                        }
                )
                .setTopics(topicName)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }

    public static MySqlSource<String> getMySqlSource(String databaseName, String tableName) {
        //允许用户不进行ssl认证，使用密码登录
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSOL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .databaseList(databaseName)//库名
                .tableList(databaseName + "." + tableName)//表名，需要带库名
                .deserializer(new JsonDebeziumDeserializationSchema())//反序列化为json格式
                .startupOptions(StartupOptions.initial())//推荐使用初始化读取方式，一开始会将整张表读一遍，后续再对变更数据进行抓取
                .build();

        return mySqlSource;
    }
}
