package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.HashSet;
import java.util.Set;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/4 21:27
 */
@Data
@AllArgsConstructor
@Builder
public class InteractionVideoPlayBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;

    //join video_info
    String chapterId;

    //join chapter_info
    String chapterName;

    //video_id
    //no sink
    @NoSink
    String videoId;

    //视频播放次数
    @Builder.Default
    Long playCount = 0L;

    //累计播放时长
    @Builder.Default
    Double playSecSum = 0D;

    //set<String> userId
    @NoSink
    @Builder.Default
    Set<String> userId = new HashSet<>();

    @Builder.Default
    Long viewerCount = 0L;

    //人均观看时长
    @Builder.Default
    Double avgSecPerViewer = 0D;

    // 时间戳
    Long ts;
}
