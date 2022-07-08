package com.atguigu.bean;

import com.atguigu.annotation.NotSink;

import java.io.Serializable;
import java.util.Set;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/6 18:27
 */
public class TrafficSourceUV implements Serializable {
    private String stt;
    private String edt;
    private String source;
    private Long count;

    private Long ts;

    @NotSink
    private Set<String> userIdSet;

    public TrafficSourceUV() {
    }

    public TrafficSourceUV(String stt, String edt, String source, Long count, Long ts, Set<String> userIdSet) {
        this.stt = stt;
        this.edt = edt;
        this.source = source;
        this.count = count;
        this.ts = ts;
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

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Set<String> getUserIdSet() {
        return userIdSet;
    }

    public void setUserIdSet(Set<String> userIdSet) {
        this.userIdSet = userIdSet;
    }

    @Override
    public String toString() {
        return "TrafficSourceUV{" +
                "stt='" + stt + '\'' +
                ", edt='" + edt + '\'' +
                ", source='" + source + '\'' +
                ", count=" + count +
                ", ts=" + ts +
                ", userIdSet=" + userIdSet +
                '}';
    }
}
