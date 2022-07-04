package com.atguigu.app;


import com.atguigu.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

public abstract class BaseAppV2 {
    /**
     *
     * @param port             端口号
     * @param p                并行度
     * @param ckGroupIdJobName ck路径 消费者 jobName
     * @param firstTopic       要消费的第一个topic
     * @param otherTopics      要消费的其它topic
     */
    public void init(int port, int p, String ckGroupIdJobName, String firstTopic, String... otherTopics) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
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

        List<String> topics = new ArrayList<>(Arrays.asList(otherTopics));
        topics.add(firstTopic);

        Map<String, DataStreamSource<String>> streams = new HashMap<>();
        for (String topic : topics) {
            DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckGroupIdJobName, topic));
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
