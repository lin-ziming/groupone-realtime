package com.atguigu.bean;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/5 10:44
 */
public class SessionSc {
    private String sessionId;
    private Long sc;

    public SessionSc() {
    }

    public SessionSc(String sessionId, Long sc) {
        this.sessionId = sessionId;
        this.sc = sc;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Long getSc() {
        return sc;
    }

    public void setSc(Long sc) {
        this.sc = sc;
    }

    @Override
    public String toString() {
        return "SessionSc{" +
                "sessionId='" + sessionId + '\'' +
                ", sc=" + sc +
                '}';
    }
}
