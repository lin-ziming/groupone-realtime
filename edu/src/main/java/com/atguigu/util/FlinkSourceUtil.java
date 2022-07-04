package com.atguigu.util;

import com.atguigu.common.Constant;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author shogunate
 * @description FlinkSourceUtil
 * @date 2022/7/4 10:58
 */
public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return KafkaSource
            .<String>builder()
            .setTopics(topic)
            .setGroupId(groupId)
//            .setValueOnlyDeserializer()
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
            .setBootstrapServers(Constant.KAFKA_BROKERS)
            .setDeserializer(new KafkaRecordDeserializationSchema<String>() {
                @Override
                public void deserialize (ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws IOException {
                    if (record.value() == null) {
                        out.collect(null);
                    }
                    out.collect(new String(record.value(), StandardCharsets.UTF_8));
                }

                @Override
                public TypeInformation<String> getProducedType ( ) {
                    return Types.STRING;
                }
            })
            .build();

    }
}
