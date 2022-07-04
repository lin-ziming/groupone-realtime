package com.atguigu.app;

import com.atguigu.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shogunate
 * @description FlinkDemo to test flink and hdfs connection
 * @date 2022/7/4 11:11
 */
public class FlinkDemo extends BaseApp{

    public static void main(String[] args) {
        new FlinkDemo().init(2, 10041, "FlinkDemo", Constant.TOPIC_ODS_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
