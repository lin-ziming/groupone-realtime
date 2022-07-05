package com.atguigu.util;

import com.atguigu.common.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FlinkSourceUtil {
    private static Properties setKafkaProperties(String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        properties.put("group.id", groupId);
        properties.put("isolation.level", "read_committed");
        return properties;
    }

    private static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, Properties props) {
        return new FlinkKafkaConsumer<String>(
                topic,
                /*new SimpleStringSchema()*/
                // 自定义反序列化器
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) {
                        if (record.value() == null) {
                            return null;
                        }
                        return new String(record.value(), StandardCharsets.UTF_8);
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                },
                props);
    }

    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {

        Properties props = setKafkaProperties(groupId);
        return getKafkaConsumer(topic, props);
    }

    /**
     * 指定各分区开始消费的位置
     * @param groupId
     * @param topic
     * @param startOffsets 各分区开始消费的位置
     * @return
     */
    public static SourceFunction<String> getKafkaSource(String groupId, String topic, Long[] startOffsets) {

        Properties props = setKafkaProperties(groupId);
        FlinkKafkaConsumer<String> myConsumer = getKafkaConsumer(topic, props);

        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        for (int i = 0; i < startOffsets.length; i++) {
            specificStartOffsets.put(new KafkaTopicPartition(topic, i), startOffsets[i]);
        }
        return myConsumer.setStartFromSpecificOffsets(specificStartOffsets);
    }

    public static MySqlSource<String> getFlinkCDCSource() {
        return MySqlSource.<String>builder()
            .hostname("hadoop302")
            .password("123456")
            .username("root")
            .databaseList("gmall_config")
            .port(3306)
            .tableList("gmall_config.table_process_edu_dim")
            .deserializer(new JsonDebeziumDeserializationSchema())
            .startupOptions(StartupOptions.initial())
            .build();
    }
}
