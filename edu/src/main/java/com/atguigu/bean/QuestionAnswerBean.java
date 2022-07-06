package com.atguigu.bean;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
@AllArgsConstructor
@Data
@NoArgsConstructor
@Builder
public class QuestionAnswerBean {
        // 窗口起始时间
        String stt;
        // 窗口结束时间
        String edt;

        String questionId;

        //正确答题次数
        @Builder.Default
        Integer correctCt = 0;

        //答题次数
        @Builder.Default
        Integer answerCt = 0;

        //正确率
        @Builder.Default
        Double correctPercent = 0D;

//        //正确答题独立用户数
//        @Builder.Default
//        Set<String>  correctUV= new HashSet<>();
//
//        Integer correctUvCt = 0 ;
//
//        //答题独立用户数
//        @Builder.Default
//        Set<String>  answerUV= new HashSet<>();
//
//        Integer answerUvCt = 0 ;
//
//
//        //正确答题用户占比
//        @Builder.Default
//        Double correctUVPercent= 0D;

        // 时间戳
        Long ts;


}
