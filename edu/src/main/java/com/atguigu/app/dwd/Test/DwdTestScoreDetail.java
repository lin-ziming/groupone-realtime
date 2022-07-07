package com.atguigu.app.dwd.Test;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTestScoreDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTestScoreDetail().init(2202, 2, "DwdTestScoreDetail", 10);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        readOdsDb(tEnv, "DwdTestScoreDetail");

        Table test_exam = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['paper_id'] paper_id,  " +
                        " `data`['user_id'] user_id, " +
                        " `data`['score'] score, " +
                        " cast(`data`['duration_sec'] as bigint) duration_sec, " +
                        " `data`['create_time'] create_time, " +
                        " `data`['submit_time'] submit_time, " +
                        " `data`['update_time'] update_time, " +
                        " `data`['deleted'] deleted, " +
                        " ts, " +
                        " pt " +
                        " from ods_db " +
                        " where `database`='gmall' " +
                        " and `table`='test_exam' " +
                        " and `type`='insert' "
        );
//        test_exam.execute().print();
        tEnv.createTemporaryView("test_exam", test_exam);

        Table test_exam_question = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['exam_id'] exam_id, " +
                        " `data`['paper_id'] paper_id, " +
                        " `data`['question_id'] question_id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['answer'] answer, " +
                        " `data`['is_correct'] is_correct, " +
                        " `data`['score'] score, " +
                        " `data`['create_time'] create_time, " +
                        " `data`['update_time'] update_time, " +
                        " `data`['deleted'] deleted, " +
                        " pt " +
                        " from ods_db " +
                        " where `database`='gmall' " +
                        " and `table`='test_exam_question' " +
                        " and `type`='insert' "
        );
//        test_exam_question.execute().print();
        tEnv.createTemporaryView("test_exam_question", test_exam_question);

        Table test_paper = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['paper_title'] paper_title, " +
                        " `data`['course_id'] course_id, " +
                        " `data`['create_time'] create_time, " +
                        " `data`['update_time'] update_time, " +
                        " `data`['publisher_id'] publisher_id, " +
                        " `data`['deleted'] deleted, " +
                        " pt " +
                        " from ods_db " +
                        " where `database`='gmall' " +
                        " and `table`='test_paper' "
        );
        tEnv.createTemporaryView("test_paper", test_paper);

        Table result = tEnv.sqlQuery(
                "select " +
                        " te.id, " +
                        " te.paper_id, " +
                        " te.user_id, " +
                        " te.duration_sec, " +
                        " te.score, " +
                        " tq.question_id, " +
                        " tq.answer, " +
                        " tq.is_correct, " +
                        " tq.score, " +
                        " tp.course_id," +
                        " te.ts, " +
                        " te.pt " +
                        "from test_exam te " +
                        "join test_exam_question tq on te.id=tq.exam_id " +
                        "join test_paper tp on te.paper_id=tp.id "

        );

//        result.execute().print();

        tEnv.executeSql(
                "create table dwd_test_score_detail(" +
                        " `id` string, " +
                        " `paper_id` string, " +
                        " `user_id` string, " +
                        " `duration_sec` bigint, " +
                        " `total_score` string, " +
                        " `question_id` string, " +
                        " `answer` string, " +
                        " `is_correct` string, " +
                        " `score` string, " +
                        " `course_id` string, " +
                        " `ts` bigint, " +
                        " pt TIMESTAMP_LTZ(3)" +
                        ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TEST_SCORE_DETAIL)
        );

        result.executeInsert("dwd_test_score_detail");
    }
}

