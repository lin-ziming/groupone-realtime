package com.example.sugar.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradeCategory {
    private String category_name;
    private long  order_count;
    private long  uu_count;
    private double  order_amount;
}
