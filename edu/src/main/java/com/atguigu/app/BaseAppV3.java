package com.atguigu.app;


import com.atguigu.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

public abstract class BaseAppV3 {
    /**
     * 指定消费的topic与offsets
     */
    public void init(int port, int p, String ckGroupIdJobName, Map<String, Long[]> topicsAndOffsets) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        conf.setString("flink.hadoop.dfs.client.use.datanode.hostname","true"); //

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);

        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop302:8020/edu/" + ckGroupIdJobName);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Map<String, DataStreamSource<String>> streams = new HashMap<>();
        for (Map.Entry<String, Long[]> entry : topicsAndOffsets.entrySet()) {
            String topic = entry.getKey();
            Long[] offsets = entry.getValue();
            DataStreamSource<String> stream;
            if (offsets.length == 0) {
                stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckGroupIdJobName, topic));
            } else {
                stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckGroupIdJobName, topic, offsets));
            }
            streams.put(topic, stream);
        }

        handle(env, streams);

        try {
            env.execute(ckGroupIdJobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public abstract void handle(StreamExecutionEnvironment env,
                                Map<String, DataStreamSource<String>> streams);

}
