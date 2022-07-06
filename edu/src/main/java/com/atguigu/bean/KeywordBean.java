package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeywordBean {
    private String stt;
    private String edt;
    private String keyword;
    private Long keywordCount;
    private Long ts;
}
