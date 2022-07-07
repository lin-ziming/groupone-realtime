package com.atguigu.app;


import com.atguigu.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseAppV1 {
    /**
     *
     * @param port 端口号
     * @param p 并行度
     * @param ckGroupIdJobName ck路径 消费者 jobName
     * @param topic 要消费的topic
     */
    public void init(int port, int p, String ckGroupIdJobName, String topic){
        System.setProperty("HADOOP_USER_NAME","atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        //conf.setString("flink.hadoop.dfs.client.use.datanode.hostname","true"); //

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

        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckGroupIdJobName, topic));
//        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(
//            ckGroupIdJobName,
//            topic,
//            new Long[]{2000L, 2000L})); //可在此处指定每个分区的偏移量起始位置，从0分区开始指定

        handle(env, stream);

        try {
            env.execute(ckGroupIdJobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public abstract void handle(StreamExecutionEnvironment env,
                                DataStreamSource<String> stream);

}
