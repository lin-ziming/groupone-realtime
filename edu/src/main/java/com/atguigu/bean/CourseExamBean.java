package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CourseExamBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;

    String courseId;

    //set<String> userId
    @NoSink
    @Builder.Default
    Set<String> userIdSet = new HashSet<>();

    Long durationSec;


    Double score;

    //考试人数
    @Builder.Default
    Integer examNum = 0;

    //平均分
    @Builder.Default
    Double avgScore = 0D;

    //平均时长
    @Builder.Default
    Double avgDuringTime = 0D;

    // 时间戳
    Long ts;

}
