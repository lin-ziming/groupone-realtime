package com.example.sugar.service;

import com.example.sugar.bean.InteractionPlayTime;
import com.example.sugar.bean.Kw;
import com.example.sugar.bean.TrafficVisitorTypeStats;

import java.util.List;

public interface TradeService {
    //Double gmv(int date);

    
    List<TrafficVisitorTypeStats> statsTrafficVisitorTypeStats(int date);
    List<Kw> statsKw(int date);
    List<InteractionPlayTime> statsInteractionPlayTime(int date);
}
