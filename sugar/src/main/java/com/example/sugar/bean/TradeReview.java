package com.example.sugar.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradeReview {
    private String course_name;
    private double  avg_score;
    private long  uu_count;
    private double  favor_rate;
}
