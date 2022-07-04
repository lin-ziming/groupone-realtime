package com.atguigu.app;

import com.atguigu.common.Constant;
import com.atguigu.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shogunate
 * @description BaseApp
 * @date 2022/7/4 10:54
 */
public abstract class BaseApp {
    public void init(int parallel, int port, String ckAndGroupId, String topic){
        System.setProperty("HADOOP_USER_NAME","atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallel);
        env.setStateBackend(new HashMapStateBackend());

        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(Constant.CK_PATH_PREFIX + ckAndGroupId);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointTimeout(60*1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);



        DataStreamSource<String> stream = env.fromSource(FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId),
            WatermarkStrategy.<String>noWatermarks(), "Kafka Source");



        handle(env,stream);

        try {
            env.execute(ckAndGroupId);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);
}












