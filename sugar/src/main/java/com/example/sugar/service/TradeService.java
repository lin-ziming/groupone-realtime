package com.example.sugar.service;


import com.example.sugar.bean.*;

import java.util.List;

public interface TradeService {
    //Double gmv(int date);

    
    List<TrafficVisitorTypeStats> statsTrafficVisitorTypeStats(int date);
    List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date);
    List<Kw> statsKw(int date);

    List<TradeSourceOrderStats> getOrderInfoBySource(int date);
    List<TradeProvinceOrderStats> getOrderInfoByProvince(int date);
    Double getTotalAmount(int date);
    Long getUserCount(int date);
    Long getOrderCount(int date);
    //cxy
    List<UserChangeCtPerType> selectUserChangeCtPerType(int date);
    List<PageViewType> selectPageIdViewCtType(int date);
}
