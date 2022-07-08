package com.example.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficUjRate {
    // 来源
    String sc;
    // 来源网站
    String sourceSite;
    // 来源网址
    String sourceUrl;

    // 跳出率
    Double ujRate;
}