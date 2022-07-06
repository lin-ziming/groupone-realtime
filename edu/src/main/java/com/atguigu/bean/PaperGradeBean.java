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
public class PaperGradeBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;

    String paperId;

    //大于80分
    @Builder.Default
    Integer greatGroup = 0;

    //70~80分
    @Builder.Default
    Integer goodGroup = 0;

    //60~70分
    @Builder.Default
    Integer midGroup = 0;

    //60分以下
    @Builder.Default
    Integer poorGroup = 0;

    @Builder.Default
    Integer ct = 0;

    // 时间戳
    Long ts;

}