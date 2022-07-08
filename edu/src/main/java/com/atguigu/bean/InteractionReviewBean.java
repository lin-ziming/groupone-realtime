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
public class InteractionReviewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // course_id
    String courseId;
    // course名称
    String courseName;
    //用户ID
    String userId;
    // 评分
    String reviewStars;
    // 时间戳
    Long ts;

}
