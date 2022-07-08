package com.example.sugar.service;

import com.example.sugar.bean.Kw;
import com.example.sugar.bean.PageViewType;
import com.example.sugar.bean.TradeProvinceOrderStats;
import com.example.sugar.bean.TradeSourceOrderStats;
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
    public List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date) {
        return tradeMapper.statsTrafficVisitorTypeStats(date);
    }

    @Override
    public List<Kw> statsKw(int date) {
        return tradeMapper.statsKw(date);
    }

    @Override
    public List<UserChangeCtPerType> selectUserChangeCtPerType(int date) {
        List<UserChangeCtPerType> list = tradeMapper.selectUserChangeCtPerType(date);
        return tradeMapper.selectUserChangeCtPerType(date);
    }

    @Override
    public List<PageViewType> selectPageIdViewCtType(int date) {
        return tradeMapper.selectPageIdViewCtType(date);
    public List<TradeSourceOrderStats> getOrderInfoBySource(int date) {
        return tradeMapper.getOrderInfoBySource(date);
    }

    @Override
    public List<TradeProvinceOrderStats> getOrderInfoByProvince(int date) {
        return tradeMapper.getOrderInfoByProvince(date);
    }

    @Override
    public Double getTotalAmount(int date) {
        return tradeMapper.getTotalAmount(date);
    }

    @Override
    public Long getUserCount(int date) {
        return tradeMapper.getUserCount(date);
    }

    @Override
    public Long getOrderCount(int date) {
        return tradeMapper.getOrderCount(date);
    }


}
