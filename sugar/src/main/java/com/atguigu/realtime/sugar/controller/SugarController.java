package com.atguigu.realtime.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.sugar.bean.CourseExam;
import com.atguigu.realtime.sugar.bean.PaperExam;
import com.atguigu.realtime.sugar.bean.PaperGrade;
import com.atguigu.realtime.sugar.bean.QuestionAnswer;
import com.atguigu.realtime.sugar.service.TestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class SugarController {
    @Autowired
    TestService testService;

    @RequestMapping("/sugar/scoretime/course")
    public String scoreTimeByCourse(int date){

        List<CourseExam> courseExamList = testService.scoreTimeByCourse(date);

        JSONObject courseResult = new JSONObject();
        courseResult.put("status", 0);
        courseResult.put("msg", "各课程考试统计");

        JSONObject courseData = new JSONObject();
        JSONArray courseCategories = new JSONArray();
        
        for (CourseExam courseExam : courseExamList) {
            courseCategories.add(courseExam.getCourse_id());
        }


        JSONArray courseSeries = new JSONArray();

        JSONObject courseCodeOne = new JSONObject();

        courseCodeOne.put("name", "平均成绩");
        courseCodeOne.put("type", "bar");
        courseCodeOne.put("yAxisIndex", "0");

        JSONArray courseDataOne = new JSONArray();
        for (CourseExam courseExam : courseExamList) {
            courseDataOne.add( courseExam.getAvg_scores());
        }

        courseCodeOne.put("data", courseDataOne);

        JSONObject courseCodeTwo = new JSONObject();

        courseCodeTwo.put("name", "平均用时");
        courseCodeTwo.put("type", "line");
        courseCodeTwo.put("yAxisIndex", "1");

        JSONArray courseDataTwo = new JSONArray();
        for (CourseExam courseExam : courseExamList) {
            courseDataTwo.add( courseExam.getAvg_duration_sec());
        }

        courseCodeTwo.put("data", courseDataTwo);


        courseSeries.add(courseCodeOne);
        courseSeries.add(courseCodeTwo);
        courseData.put("series", courseSeries);
        courseData.put("categories", courseCategories);
        courseResult.put("data", courseData);

        return courseResult.toJSONString();

    }

    @RequestMapping("/sugar/scoretime/paper")
    public String scoreTimeByPaper(int date){

        List<PaperExam> paperExamList = testService.scoreTimeByPaper(date);

        JSONObject paperResult = new JSONObject();
        paperResult.put("status", 0);
        paperResult.put("msg", "各试卷考试统计");

        JSONObject paperData = new JSONObject();
        JSONArray paperCategories = new JSONArray();

        for (PaperExam paperExam : paperExamList) {
            paperCategories.add(paperExam.getPaper_id());
        }


        JSONArray paperSeries = new JSONArray();

        JSONObject paperCodeOne = new JSONObject();

        paperCodeOne.put("name", "平均成绩");
        paperCodeOne.put("type", "bar");
        paperCodeOne.put("yAxisIndex", "0");

        JSONArray paperDataOne = new JSONArray();
        for (PaperExam paperExam : paperExamList) {
            paperDataOne.add( paperExam.getAvg_scores());
        }

        paperCodeOne.put("data", paperDataOne);

        JSONObject paperCodeTwo = new JSONObject();

        paperCodeTwo.put("name", "平均用时");
        paperCodeTwo.put("type", "line");
        paperCodeTwo.put("yAxisIndex", "1");

        JSONArray paperDataTwo = new JSONArray();
        for (PaperExam paperExam : paperExamList) {
            paperDataTwo.add( paperExam.getAvg_duration_sec());
        }

        paperCodeTwo.put("data", paperDataTwo);


        paperSeries.add(paperCodeOne);
        paperSeries.add(paperCodeTwo);
        paperData.put("series", paperSeries);
        paperData.put("categories", paperCategories);
        paperResult.put("data", paperData);


        return paperResult.toJSONString();

    }

    @RequestMapping("/sugar/papergrade")
    public String paperGrade(int date){
        List<PaperGrade> paperGradeList = testService.paperGrade(date);

        JSONObject gradeResult = new JSONObject();
        gradeResult.put("status", 0);
        gradeResult.put("msg", "各试卷成绩分布");
        JSONObject gradeData = new JSONObject();

        JSONArray gradeCategories = new JSONArray();
        for (PaperGrade paperGrade : paperGradeList) {
            gradeCategories.add(paperGrade.getPaper_id());
        }

        JSONArray gradeSeries = new JSONArray();

        JSONObject greatData = new JSONObject();
        greatData.put("name", "70~80分");
        JSONArray greatGroupData = new JSONArray();
        for (PaperGrade paperGrade : paperGradeList) {
            greatGroupData.add(paperGrade.getGood());
        }
        greatData.put("data", greatGroupData);

        JSONObject goodData = new JSONObject();
        goodData.put("name", "70~80分");
        JSONArray goodGroupData = new JSONArray();
        for (PaperGrade paperGrade : paperGradeList) {
            goodGroupData.add(paperGrade.getGood());
        }
        goodData.put("data", goodGroupData);

        JSONObject midData = new JSONObject();
        midData.put("name", "60~70分");
        JSONArray midGroupData = new JSONArray();
        for (PaperGrade paperGrade : paperGradeList) {
            midGroupData.add(paperGrade.getMid());
        }
        midData.put("data", midGroupData);

        JSONObject poorData = new JSONObject();
        poorData.put("name", "60分以下");
        JSONArray poorGroupData = new JSONArray();
        for (PaperGrade paperGrade : paperGradeList) {
            poorGroupData.add(paperGrade.getPoor());
        }
        poorData.put("data", poorGroupData);


        gradeSeries.add(poorData);
        gradeSeries.add(midData);
        gradeSeries.add(goodData);
        gradeSeries.add(greatData);

        gradeData.put("series", gradeSeries);
        gradeData.put("categories", gradeCategories);

        gradeResult.put("data", gradeData);

        return gradeResult.toJSONString();
    }

    @RequestMapping("/sugar/questionanswer")
    public String questionAnswer(int date){

        List<QuestionAnswer> qaList = testService.questionAnswer(date);

        JSONObject qaResult = new JSONObject();
        qaResult.put("status", 0);
        qaResult.put("msg", "答题情况统计");

        JSONObject qaData = new JSONObject();

        JSONArray qaCategories = new JSONArray();

        for (QuestionAnswer questionAnswer : qaList) {
            qaCategories.add(questionAnswer.getQuestion_id());
        }


        JSONArray qaSeries = new JSONArray();

        JSONObject qaCodeOne = new JSONObject();

        qaCodeOne.put("name", "正确答题次数");
        qaCodeOne.put("type", "bar");
        qaCodeOne.put("yAxisIndex", "0");

        JSONArray qaDataOne = new JSONArray();
        for (QuestionAnswer questionAnswer : qaList) {
            qaDataOne.add(questionAnswer.getCorrect_ct());
        }

        qaCodeOne.put("data", qaDataOne);



        JSONObject qaCodeTwo = new JSONObject();

        qaCodeTwo.put("name", "答题次数");
        qaCodeTwo.put("type", "bar");
        qaCodeTwo.put("yAxisIndex", "0");

        JSONArray qaDataTwo = new JSONArray();
        for (QuestionAnswer questionAnswer : qaList) {
            qaDataTwo.add(questionAnswer.getAnswer_ct());
        }

        qaCodeTwo.put("data", qaDataTwo);



        JSONObject qaCodeThree = new JSONObject();

        qaCodeThree.put("name", "正确率");
        qaCodeThree.put("type", "line");
        qaCodeThree.put("yAxisIndex", "1");

        JSONArray qaDataThree = new JSONArray();
        for (QuestionAnswer questionAnswer : qaList) {
            qaDataThree.add(questionAnswer.getCorrect_percent());
        }

        qaCodeThree.put("data", qaDataThree);


        JSONObject qaCodeFour = new JSONObject();

        qaCodeFour.put("name", "正确答题独立用户数");
        qaCodeFour.put("type", "bar");
        qaCodeFour.put("yAxisIndex", "0");

        JSONArray qaDataFour = new JSONArray();
        for (QuestionAnswer questionAnswer : qaList) {
            qaDataFour.add(questionAnswer.getCorrect_uv_ct());
        }

        qaCodeFour.put("data", qaDataFour);



        JSONObject qaCodeFive = new JSONObject();

        qaCodeFive.put("name", "答题独立用户数");
        qaCodeFive.put("type", "bar");
        qaCodeFive.put("yAxisIndex", "0");

        JSONArray qaDataFive = new JSONArray();
        for (QuestionAnswer questionAnswer : qaList) {
            qaDataFive.add(questionAnswer.getAnswer_uv_ct());
        }

        qaCodeFive.put("data", qaDataFive);


        JSONObject qaCodeSix = new JSONObject();

        qaCodeSix.put("name", "正确答题用户占比");
        qaCodeSix.put("type", "line");
        qaCodeSix.put("yAxisIndex", "1");

        JSONArray qaDataSix = new JSONArray();
        for (QuestionAnswer questionAnswer : qaList) {
            qaDataSix.add(questionAnswer.getCorrect_uv_percent());
        }

        qaCodeSix.put("data", qaDataSix);



        qaSeries.add(qaCodeOne);
        qaSeries.add(qaCodeTwo);
        qaSeries.add(qaCodeThree);
        qaSeries.add(qaCodeFour);
        qaSeries.add(qaCodeFive);
        qaSeries.add(qaCodeSix);

        qaData.put("series", qaSeries);
        qaData.put("categories", qaCategories);
        qaResult.put("data", qaData);

        return qaResult.toJSONString();


    }


}

