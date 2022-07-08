package com.example.sugar.service;

import com.example.sugar.bean.TrafficVisitorStatsPerHour;

import java.util.List;

public interface TrafficVisitorStatsService {
    // 获取分时流量数据
    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);
}