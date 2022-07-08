package com.atguigu.realtime.sugar.service;

import com.atguigu.realtime.sugar.bean.CourseExam;
import com.atguigu.realtime.sugar.bean.PaperExam;
import com.atguigu.realtime.sugar.bean.PaperGrade;
import com.atguigu.realtime.sugar.bean.QuestionAnswer;

import java.util.List;

public interface TestService {

    List<CourseExam> scoreTimeByCourse(int date);

    List<PaperExam> scoreTimeByPaper(int date);

    List<PaperGrade> paperGrade (int date);

    List<QuestionAnswer> questionAnswer (int date);






}
