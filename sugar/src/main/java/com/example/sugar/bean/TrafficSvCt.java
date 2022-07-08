package com.example.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class TrafficSvCt {
    // 来源
    String sc;
    // 来源网站
    String sourceSite;
    // 来源网址
    String sourceUrl;

    // 会话数
    Integer svCt;
}