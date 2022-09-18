package com.example.sugar.bean;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/8 12:01
 */
public class TradeSourceOrderStats {
    private String source;
    private Double amount;
    private Long userCount;
    private Long orderCount;
    private Double convertRate;

    public TradeSourceOrderStats() {
    }

    public TradeSourceOrderStats(String source, Double amount, Long userCount, Long orderCount, Double convertRate) {
        this.source = source;
        this.amount = amount;
        this.userCount = userCount;
        this.orderCount = orderCount;
        this.convertRate = convertRate;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Long getUserCount() {
        return userCount;
    }

    public void setUserCount(Long userCount) {
        this.userCount = userCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Double getConvertRate() {
        return convertRate;
    }

    public void setConvertRate(Double convertRate) {
        this.convertRate = convertRate;
    }

    @Override
    public String toString() {
        return "TradeSourceOrderStats{" +
                "source='" + source + '\'' +
                ", amount=" + amount +
                ", userCount=" + userCount +
                ", orderCount=" + orderCount +
                ", convertRate=" + convertRate +
                '}';
    }
}
