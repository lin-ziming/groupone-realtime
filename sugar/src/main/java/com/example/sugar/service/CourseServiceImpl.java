package com.example.sugar.service;

import com.example.sugar.bean.TradeCourse;
import com.example.sugar.bean.TradeReview;
import com.example.sugar.bean.TradeSubject;
import com.example.sugar.bean.TradeCategory;
import com.example.sugar.mapper.CourseMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CourseServiceImpl implements CourseService {

    @Autowired
    CourseMapper courseMapper;

    @Override
    public List<TradeCourse> tradeCourse(int date) {
        return courseMapper.tradeCourse(date);
    }

    @Override
    public List<TradeSubject> tradeSubject(int date) {
        return courseMapper.tradeSubject(date);
    }

    @Override
    public List<TradeCategory> tradeCategory(int date) {
        return courseMapper.tradeCategory(date);
    }

    @Override
    public List<TradeReview> tradeReview(int date) {
        return courseMapper.tradeReview(date);
    }

}
