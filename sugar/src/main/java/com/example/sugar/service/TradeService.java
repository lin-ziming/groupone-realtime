package com.example.sugar.service;

import com.example.sugar.bean.Kw;
import com.example.sugar.bean.PageViewType;
import com.example.sugar.bean.TrafficVisitorTypeStats;
import com.example.sugar.bean.UserChangeCtPerType;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface TradeService {
    //Double gmv(int date);

    
    List<TrafficVisitorTypeStats> statsTrafficVisitorTypeStats(int date);
    List<Kw> statsKw(int date);
    List<UserChangeCtPerType> selectUserChangeCtPerType(int date);
    List<PageViewType> selectPageIdViewCtType(int date);
}
