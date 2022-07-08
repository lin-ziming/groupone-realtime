package com.example.sugar.service;

import com.example.sugar.bean.TradeCourse;
import com.example.sugar.bean.TradeReview;
import com.example.sugar.bean.TradeSubject;
import com.example.sugar.bean.TradeCategory;
import java.util.List;

public interface CourseService {
    List<TradeCourse> tradeCourse(int date);
    List<TradeSubject> tradeSubject(int date);
    List<TradeCategory> tradeCategory(int date);
    List<TradeReview> tradeReview(int date);

}
