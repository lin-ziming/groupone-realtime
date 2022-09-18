package com.atguigu.realtime.sugar.service;

import com.atguigu.realtime.sugar.bean.CourseExam;
import com.atguigu.realtime.sugar.bean.PaperExam;
import com.atguigu.realtime.sugar.bean.PaperGrade;
import com.atguigu.realtime.sugar.bean.QuestionAnswer;
import com.atguigu.realtime.sugar.mapper.TestMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
public class TestServiceImpl implements TestService{

    @Autowired
    TestMapper testMapper;

    @Override
    public List<CourseExam> scoreTimeByCourse(int date) {
        return testMapper.scoreTimeByCourse(date);
    }


    @Override
    public List<PaperExam> scoreTimeByPaper(int date) {
        return testMapper.scoreTimeByPaper(date);
    }

    @Override
    public List<PaperGrade> paperGrade(int date) {
        return testMapper.paperGrade(date);
    }

    @Override
    public List<QuestionAnswer> questionAnswer(int date) {
        return testMapper.questionAnswer(date);
    }
}
