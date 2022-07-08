package com.example.sugar.bean;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/8 13:31
 */
public class TradeProvinceOrderStats {
    private String province;
    private Double amount;
    private Long userCount;
    private Long orderCount;

    public TradeProvinceOrderStats() {
    }

    public TradeProvinceOrderStats(String province, Double amount, Long userCount, Long orderCount) {
        this.province = province;
        this.amount = amount;
        this.userCount = userCount;
        this.orderCount = orderCount;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
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

    @Override
    public String toString() {
        return "TradeProvinceOrderStats{" +
                "province='" + province + '\'' +
                ", amount=" + amount +
                ", userCount=" + userCount +
                ", orderCount=" + orderCount +
                '}';
    }
}
