package com.atguigu.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author shogunate
 * @description HDFSClient to test hdfs file put and poll
 * @date 2022/7/4 11:35
 */
public class HDFSClient {
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException, IOException {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");
        // FileSystem fs = FileSystem.get(configuration);


        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop302:8020"), configuration, "atguigu");

        // 2 创建目录
//        fs.mkdirs(new Path("/test/"));

        //put
        fs.copyFromLocalFile(new Path("testDatas/3.txt"), new Path("/test/3.txt"));

        // 3 关闭资源
        fs.close();
        System.out.println("end");
    }
}
