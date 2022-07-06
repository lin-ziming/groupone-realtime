package com.atguigu.bean;

import java.util.Set;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/6 18:16
 */
public class TradeSourceProvinceOrder {
    private String stt;
    private String edt;
    private String source;
    private String province;

    // 下单人数
    private Long count;

    // 下单次数
    private Long times;

    // 下单金额
    private Double amount;

    private Long ts;

    @NoSink
    private Set<String> orderIdSet;
    @NoSink
    private Set<String> userIdSet;

    public TradeSourceProvinceOrder() {
    }

    public TradeSourceProvinceOrder(String stt, String edt, String source, String province, Long count, Long times, Double amount, Long ts, Set<String> orderIdSet, Set<String> userIdSet) {
        this.stt = stt;
        this.edt = edt;
        this.source = source;
        this.province = province;
        this.count = count;
        this.times = times;
        this.amount = amount;
        this.ts = ts;
        this.orderIdSet = orderIdSet;
        this.userIdSet = userIdSet;
    }

    public String getStt() {
        return stt;
    }

    public void setStt(String stt) {
        this.stt = stt;
    }

    public String getEdt() {
        return edt;
    }

    public void setEdt(String edt) {
        this.edt = edt;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getTimes() {
        return times;
    }

    public void setTimes(Long times) {
        this.times = times;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Set<String> getOrderIdSet() {
        return orderIdSet;
    }

    public void setOrderIdSet(Set<String> orderIdSet) {
        this.orderIdSet = orderIdSet;
    }

    public Set<String> getUserIdSet() {
        return userIdSet;
    }

    public void setUserIdSet(Set<String> userIdSet) {
        this.userIdSet = userIdSet;
    }

    @Override
    public String toString() {
        return "TradeSourceProvinceOrder{" +
                "stt='" + stt + '\'' +
                ", edt='" + edt + '\'' +
                ", source='" + source + '\'' +
                ", province='" + province + '\'' +
                ", count=" + count +
                ", times=" + times +
                ", amount=" + amount +
                ", ts=" + ts +
                ", orderIdSet=" + orderIdSet +
                ", userIdSet=" + userIdSet +
                '}';
    }
}
