package com.atguigu.realtime.sugar.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaperGrade {
    String paper_id;
    String great;
    String good;
    String mid;
    String poor;
}
