package com.example.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficVisitorStatsPerHour {
    // 小时
    Integer hr;
    // 独立访客数
    Long uvCt;
    // 页面浏览数
    Long pvCt;
    // 会话平均浏览页面数
    Double pvPerSession;
}