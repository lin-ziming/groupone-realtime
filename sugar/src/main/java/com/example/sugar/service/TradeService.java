package com.example.sugar.service;

import com.example.sugar.bean.Kw;
import com.example.sugar.bean.TradeProvinceOrderStats;
import com.example.sugar.bean.TradeSourceOrderStats;
import com.example.sugar.bean.TrafficVisitorTypeStats;

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
}
