package com.example.sugar.service;

import com.example.sugar.bean.TrafficVisitorStatsPerHour;
import com.example.sugar.mapper.TrafficVisitorStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficVisitorStatsServiceImpl implements TrafficVisitorStatsService {

    @Autowired
    private TrafficVisitorStatsMapper trafficVisitorStatsMapper;

	// 获取分时流量统计数据
    @Override
    public List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorStatsPerHr(date);
    }
}