package com.atguigu.bean;

import java.io.Serializable;

/**
 * @Author: Iris_Liu
 * @Description: todo
 * @Create_time: 2022/7/8 9:39
 */
public class BaseSource implements Serializable {
    private String sourceId;
    private String sourceName;

    public BaseSource() {
    }

    public BaseSource(String sourceId, String sourceName) {
        this.sourceId = sourceId;
        this.sourceName = sourceName;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public String toString() {
        return "BaseSource{" +
                "sourceId='" + sourceId + '\'' +
                ", sourceName='" + sourceName + '\'' +
                '}';
    }
}
