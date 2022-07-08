package com.atguigu.realtime.sugar.mapper;

import com.atguigu.realtime.sugar.bean.CourseExam;
import com.atguigu.realtime.sugar.bean.PaperExam;
import com.atguigu.realtime.sugar.bean.PaperGrade;
import com.atguigu.realtime.sugar.bean.QuestionAnswer;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TestMapper {

//    select course_id, sum(duration_sec)/sum(exam_num) avg_duration_sec, sum(score)/sum(exam_num) avg_scores from dws_test_course_exam_window where toYYYYMMDD(stt)=20220704 group by course_id;
//    select paper_id, sum(duration_sec)/sum(exam_num) avg_duration_sec, sum(score)/sum(exam_num) avg_scores from dws_test_paper_exam_window where toYYYYMMDD(stt)=20220704 group by paper_id;
//    select paper_id, sum(great_group) great , sum(good_group) good , sum(mid_group) mid , sum(poor_group) poor from dws_test_paper_grade_window where toYYYYMMDD(stt)=20220704 group by paper_id;
//    select question_id, sum(correct_ct) correct_ct , sum(answer_ct) answer_ct , sum(correct_uv_ct) correct_uv_ct , sum(answer_uv_ct) answer_uv_ct , correct_ct/answer_ct correct_percent,correct_uv_ct/answer_uv_ct correct_uv_percent from dws_test_question_answer_window where toYYYYMMDD(stt)=20220704 group by question_id;


    @Select("SELECT  " +
             "    course_id, " +
             "    sum(duration_sec) / sum(exam_num) AS avg_duration_sec, " +
             "    sum(score) / sum(exam_num) AS avg_scores " +
             "FROM dws_test_course_exam_window " +
             "WHERE toYYYYMMDD(stt) = #{date} " +
             "GROUP BY course_id ")

    List<CourseExam> scoreTimeByCourse(int date);

     @Select("SELECT  " +
             "    paper_id, " +
             "    sum(duration_sec) / sum(exam_num) AS avg_duration_sec, " +
             "    sum(score) / sum(exam_num) AS avg_scores " +
             "FROM dws_test_paper_exam_window " +
             "WHERE toYYYYMMDD(stt) = #{date} " +
             "GROUP BY paper_id ")

    List<PaperExam> scoreTimeByPaper(int date);

    @Select("SELECT  " +
            "    paper_id, " +
            "    sum(great_group) AS great, " +
            "    sum(good_group) AS good, " +
            "    sum(mid_group) AS mid, " +
            "    sum(poor_group) AS poor " +
            "FROM dws_test_paper_grade_window " +
            "WHERE toYYYYMMDD(stt) = #{date} " +
            "GROUP BY paper_id ")

    List<PaperGrade> paperGrade (int date);

    @Select("SELECT  " +
            "    question_id, " +
            "    sum(correct_ct) AS correct_ct, " +
            "    sum(answer_ct) AS answer_ct, " +
            "    sum(correct_uv_ct) AS correct_uv_ct, " +
            "    sum(answer_uv_ct) AS answer_uv_ct, " +
            "    correct_ct / answer_ct AS correct_percent, " +
            "    correct_uv_ct / answer_uv_ct AS correct_uv_percent " +
            "FROM dws_test_question_answer_window " +
            "WHERE toYYYYMMDD(stt) = #{date} " +
            "GROUP BY question_id ")

    List<QuestionAnswer> questionAnswer (int date);



}
