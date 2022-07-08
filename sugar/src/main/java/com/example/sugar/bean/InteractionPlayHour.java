package com.example.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author shogunate
 * @description InteractionPlayHour
 * @date 2022/7/7 22:32
 */
@Data
@AllArgsConstructor
public class InteractionPlayHour {
    private Double play_sec_sum;
    private Double avg_sec_per_viewer;
//    private Long play_count;
//    private Long viewer_count;
    //    private String chapter_name;
    private int hour;
}

























