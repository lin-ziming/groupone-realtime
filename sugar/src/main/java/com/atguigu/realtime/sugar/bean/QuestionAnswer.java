package com.atguigu.realtime.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class QuestionAnswer {

    String question_id;
    Integer correct_ct;
    Integer answer_ct;
    Integer correct_uv_ct;
    Integer answer_uv_ct;

    Double correct_percent;
    Double correct_uv_percent;

}
