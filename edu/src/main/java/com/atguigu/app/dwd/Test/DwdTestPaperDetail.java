package com.atguigu.app.dwd.Test;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTestPaperDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTestPaperDetail().init(2016, 4, "DwdTestPaperDetail", 10);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        readOdsDb(tEnv, "DwdTestPaperDetail");

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
                        " ts, " +
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


        Table test_paper_question = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['paper_id'] paper_id, " +
                        " `data`['question_id'] question_id, " +
                        " `data`['score'] score, " +
                        " `data`['create_time'] create_time, " +
                        " `data`['publisher_id'] publisher_id, " +
                        " `data`['deleted'] deleted, " +
                        " pt " +
                        " from ods_db " +
                        " where `database`='gmall' " +
                        " and `table`='test_paper_question' "
        );
        tEnv.createTemporaryView("test_paper_question", test_paper_question);


        Table result = tEnv.sqlQuery(
                "select " +
                        " tp.id, " +
                        " tp.course_id, " +
                        " teq.exam_id, " +
                        " teq.question_id, " +
                        " teq.user_id, " +
                        " teq.answer, " +
                        " teq.is_correct, " +
                        " teq.score, " +
                        " tpq.score, " +
                        " teq.ts, " +
                        " tp.pt " +
                        " from test_exam_question teq " +
                        " join test_paper tp on teq.paper_id=tp.id " +
                        " join test_paper_question tpq on teq.question_id=tpq.question_id "


        );
//        result.execute().print();

        tEnv.executeSql(
                "create table dwd_test_paper_detail(" +
                        "paper_id string, " +
                        "course_id string, " +
                        "exam_id string, " +
                        "question_id string, " +
                        "user_id string, " +
                        "answer string, " +
                        "is_correct string, " +
                        "score string, " +
                        "score_value string, " +
                        "is_correct string, " +
                        " `ts` bigint, " +
                        " pt TIMESTAMP_LTZ(3)" +
                        ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TEST_PAPER_DETAIL)
        );

        result.executeInsert("dwd_test_paper_detail");
    }
}
