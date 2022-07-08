package com.example.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class TrafficPvPerSession {
    // 来源
    String sc;
    // 来源网站
    String sourceSite;
    // 来源网址
    String sourceUrl;

    // 各会话页面浏览数
    Double pvPerSession;
}