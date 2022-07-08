package com.atguigu.bean;

import com.atguigu.annotation.NotSink;
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
public class TradeCourseOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;


    // course_id
    String courseId;
    // course名称
    String courseName;

    // 学科id
    String subjectId;
    // 学科名称
    String subjectName;

    // 类别id
    String categoryId;
    // 类别名称
    String categoryName;

    // 订单 ID
    @NotSink
    @Builder.Default
    Set<String> orderIdSet = new HashSet<>();
    //用户ID
    String userId;
    
    // 下单次数
    @Builder.Default
    Long orderCount = 0L;
    // 下单金额
    @Builder.Default
    Double orderAmount = 0D;
    // 时间戳
    Long ts;

}
