package com.example.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserChangeCtPerType {
    // 变动类型
    String type;
    // 用户数
    Integer registerCt;
    
}
