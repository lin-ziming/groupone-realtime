package com.example.sugar.service;

import com.example.sugar.bean.Kw;
import com.example.sugar.bean.TradeProvinceOrderStats;
import com.example.sugar.bean.TradeSourceOrderStats;
import com.example.sugar.bean.TrafficVisitorTypeStats;
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
    public List<TradeSourceOrderStats> getOrderInfoBySource(int date) {
        return tradeMapper.getOrderInfoBySource(date);
    }

    @Override
    public List<TradeProvinceOrderStats> getOrderInfoByProvince(int date) {
        return tradeMapper.getOrderInfoByProvince(date);
    }


}
