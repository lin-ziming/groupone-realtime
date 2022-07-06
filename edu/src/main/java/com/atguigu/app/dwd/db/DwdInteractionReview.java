package com.atguigu.app.dwd.db;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class DwdInteractionReview extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionReview().init(
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

        //过滤出课程评价表
        Table reviewInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id,  " +
                                            "data['user_id'] user_id,  " +
                                            "data['course_id'] course_id,  " +
                                            "data['review_txt'] review_txt, " +
                                            "data['review_stars'] review_stars,  " +
                                            "date_format(data['create_time'],'yyyy-MM-dd') date_id,  " +
                                            "data['create_time'] create_time,  " +
                                            "cast(ts as string) ts  " +
                                            "from ods_db " +
                                            "where `database`='gmall' " +
                                            "and `table`='review_info' " +
                                            "and `type`='insert' " +
                                            "and data['deleted']='0' " );

        // 结果写入到Kafka中
        tEnv.executeSql("create table dwd_interaction_review(  " +
                            "id string,  " +
                            "user_id string,  " +
                            "course_id string,  " +
                            "review_txt string,  " +
                            "review_stars  string,  " +
                            "data_id string,  " +
                            "create_time string,  " +
                            "ts string  " +
                            ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_INTERACTION_REVIEW));
        
        reviewInfo.executeInsert("dwd_interaction_review");

    }
}
