package com.atguigu.bean;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
@Builder
public class TestQuestionAnswerBean {
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

        //正确答题独立用户数
        @Builder.Default
        Integer correctUvCt = 0 ;

        //答题独立用户数
        Integer answerUvCt = 0 ;

        //正确答题用户占比
        @Builder.Default
        Double correctUvPercent= 0D;

        // 时间戳
        Long ts;


}
