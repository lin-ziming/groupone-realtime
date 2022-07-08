package com.example.sugar.service;

import com.example.sugar.bean.Kw;
import com.example.sugar.bean.TrafficVisitorTypeStats;
import com.example.sugar.bean.UserChangeCtPerType;
import com.example.sugar.mapper.TradeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TradeServiceImpl implements TradeService {

    @Autowired
    TradeMapper tradeMapper;

    @Override
    public List<TrafficVisitorTypeStats> statsTrafficVisitorTypeStats(int date) {
        return tradeMapper.statsTrafficVisitorTypeStats(date);
    }

    @Override
    public List<Kw> statsKw(int date) {
        return tradeMapper.statsKw(date);
    }

    @Override
    public List<UserChangeCtPerType> selectUserChangeCtPerType(int date) {
        List<UserChangeCtPerType> list = tradeMapper.selectUserChangeCtPerType(date);
        for (UserChangeCtPerType userChangeCtPerType : list) {
            System.out.println(userChangeCtPerType);
        }
        return tradeMapper.selectUserChangeCtPerType(date);
    }


}
