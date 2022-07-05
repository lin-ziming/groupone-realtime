package com.atguigu.app.dwd.db;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class DwdInteractionComment extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionComment().init(
            2015,
            2,
            "DwdInteractionReview",
            10
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        //读取ods_db数据
        readOdsDb(tEnv, "DwdInteractionReview");

        //过滤出评价表
        Table reviewInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id,  " +
                                            "data['user_id'] user_id,  " +
                                            "data['chapter_id'] chapter_id,  " +
                                            "data['course_id'] course_id,  " +
                                            "data['comment_txt'] comment_txt, " +
                                            "date_format(data['create_time'],'yyyy-MM-dd') date_id,  " +
                                            "data['create_time'] create_time,  " +
                                            "cast(ts as string) ts  " +
                                            "from ods_db " +
                                            "where `database`='gmall' " +
                                            "and `table`='comment_info' " +
                                            "and `type`='insert' " +
                                            "and data['deleted']='0' " );

        // 结果写入到Kafka中
        tEnv.executeSql("create table dwd_interaction_comment(  " +
                            "id string,  " +
                            "user_id string,  " +
                            "chapter_id  string,  " +
                            "course_id string,  " +
                            "comment_txt string,  " +
                            "data_id string,  " +
                            "create_time string,  " +
                            "ts string  " +
                            ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT));
        
        reviewInfo.executeInsert("dwd_interaction_review");

    }
}
