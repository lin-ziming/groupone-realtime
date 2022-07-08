package com.example.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sugar.bean.TradeCourse;
import com.example.sugar.bean.TradeReview;
import com.example.sugar.bean.TradeSubject;
import com.example.sugar.bean.TradeCategory;
import com.example.sugar.service.CourseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class CourseController {

    @Autowired
    CourseService courseService;

    @RequestMapping("/sugar/course")
    public String tradeCourse(int date) {

        List<TradeCourse> list = courseService.tradeCourse(date);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");

        JSONObject data = new JSONObject();
        JSONArray columns = new JSONArray();
        JSONObject name = new JSONObject();
        JSONObject order = new JSONObject();
        JSONObject uu = new JSONObject();
        JSONObject amount = new JSONObject();
        name.put("name", "课程名称");
        name.put("id", "course_name");
        order.put("name", "下单次数");
        order.put("id", "order_count");
        uu.put("name", "下单人数");
        uu.put("id", "uu_count");
        amount.put("name", "下单金额");
        amount.put("id", "order_amount");
        columns.add(name);
        columns.add(order);
        columns.add(uu);
        columns.add(amount);

        data.put("columns", columns);

        JSONArray rows = new JSONArray();

        for (TradeCourse tc : list) {
            JSONObject data1 = new JSONObject();
            rows.add(data1);
            data1.put("course_name",tc.getCourse_name());
            data1.put("order_count",tc.getOrder_count());
            data1.put("uu_count",tc.getUu_count());
            data1.put("order_amount",tc.getOrder_amount());
        }

        data.put("rows",rows);

        result.put("data", data);
        return result.toJSONString();
    }

    @RequestMapping("/sugar/subject")
    public String tradeSubject(int date) {

        List<TradeSubject> list = courseService.tradeSubject(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray columns = new JSONArray();
        JSONObject name = new JSONObject();
        JSONObject order = new JSONObject();
        JSONObject uu = new JSONObject();
        JSONObject amount = new JSONObject();
        name.put("name", "学科名称");
        name.put("id", "subject_name");
        order.put("name", "下单次数");
        order.put("id", "order_count");
        uu.put("name", "下单人数");
        uu.put("id", "uu_count");
        amount.put("name", "下单金额");
        amount.put("id", "order_amount");
        columns.add(name);
        columns.add(order);
        columns.add(uu);
        columns.add(amount);

        data.put("columns", columns);

        JSONArray rows = new JSONArray();

        for (TradeSubject tc : list) {
            JSONObject data1 = new JSONObject();
            rows.add(data1);
            data1.put("subject_name", tc.getSubject_name());
            data1.put("order_count", tc.getOrder_count());
            data1.put("uu_count", tc.getUu_count());
            data1.put("order_amount", tc.getOrder_amount());
        }

        data.put("rows", rows);

        result.put("data", data);
        return result.toJSONString();
    }

    @RequestMapping("/sugar/category")
    public String tradeCategory(int date) {

        List<TradeCategory> list = courseService.tradeCategory(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray columns = new JSONArray();
        JSONObject name = new JSONObject();
        JSONObject order = new JSONObject();
        JSONObject uu = new JSONObject();
        JSONObject amount = new JSONObject();
        name.put("name", "类别名称");
        name.put("id", "category_name");
        order.put("name", "下单次数");
        order.put("id", "order_count");
        uu.put("name", "下单人数");
        uu.put("id", "uu_count");
        amount.put("name", "下单金额");
        amount.put("id", "order_amount");
        columns.add(name);
        columns.add(order);
        columns.add(uu);
        columns.add(amount);

        data.put("columns", columns);

        JSONArray rows = new JSONArray();

        for (TradeCategory tc : list) {
            JSONObject data1 = new JSONObject();
            rows.add(data1);
            data1.put("category_name", tc.getCategory_name());
            data1.put("order_count", tc.getOrder_count());
            data1.put("uu_count", tc.getUu_count());
            data1.put("order_amount", tc.getOrder_amount());
        }

        data.put("rows", rows);

        result.put("data", data);
        return result.toJSONString();
    }

    @RequestMapping("/sugar/review")
    public String tradeReview(int date) {

        List<TradeReview> list = courseService.tradeReview(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray columns = new JSONArray();
        JSONObject name = new JSONObject();
        JSONObject order = new JSONObject();
        JSONObject uu = new JSONObject();
        JSONObject amount = new JSONObject();
        name.put("name", "课程名称");
        name.put("id", "course_name");
        order.put("name", "用户平均评分");
        order.put("id", "avg_score");
        uu.put("name", "评价用户数");
        uu.put("id", "uu_count");
        amount.put("name", "好评率(百分比)");
        amount.put("id", "favor_rate");
        columns.add(name);
        columns.add(order);
        columns.add(uu);
        columns.add(amount);

        data.put("columns", columns);

        JSONArray rows = new JSONArray();

        for (TradeReview tc : list) {
            JSONObject data1 = new JSONObject();
            rows.add(data1);
            data1.put("course_name", tc.getCourse_name());
            data1.put("avg_score", tc.getAvg_score());
            data1.put("uu_count", tc.getUu_count());
            data1.put("favor_rate", tc.getFavor_rate());
        }

        data.put("rows", rows);

        result.put("data", data);
        return result.toJSONString();
    }
}