package com.atguigu.app;

import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public abstract class BaseSQLApp {
    /**
     * @param port             端口号
     * @param p                并行度
     * @param ckGroupIdJobName ck路径 消费者 jobName
     * @param ttlSecond 事实表的超时时间
     */
    public void init(int port, int p, String ckGroupIdJobName, long ttlSecond) {
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


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 给sql应用设置job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name",ckGroupIdJobName);
        // 在join的时候, 对join双方的表均有效
        // 对lookup join的维表无效
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(ttlSecond));

        handle(env, tEnv);
    }

    protected abstract void handle(StreamExecutionEnvironment env,
                                   StreamTableEnvironment tEnv);

    public void readOdsDb(StreamTableEnvironment tEnv, String groupId){
        tEnv.executeSql("create table ods_db(\n" +
                " `database` string, \n" +
                " `table` string, \n" +
                " `type` string, \n" +
                " `ts` bigint, \n" +
                " `data` map<string, string>, \n" +
                " `old` map<string, string>, \n" +
                " pt as proctime() \n" +
                ")" + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_ODS_DB,groupId));
    }

    public void readBaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql("create table base_dic(\n" +
                " dic_code string, \n" +
                " dic_name string \n" +
                ")with(\n" +
                " 'connector' = 'jdbc', \n" +
                " 'url' = 'jdbc:mysql://hadoop302:3306/gmall', \n" +
                " 'table-name' = 'base_dic', \n" +
                " 'username' = 'root', \n" +
                " 'password' = '123456', \n" +
                " 'lookup.cache.max-rows' = '10', \n" +
                " 'lookup.cache.ttl' = '30 s' \n" +
                ")");
    }

}
