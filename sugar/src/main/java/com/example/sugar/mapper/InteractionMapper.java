package com.example.sugar.mapper;

import com.example.sugar.bean.InteractionPlayHour;
import com.example.sugar.bean.InteractionPlayTime;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface InteractionMapper {
    @Select(
        "SELECT " +
            " sum(play_sec_sum ) play_hours, " +
            " sum(avg_sec_per_viewer) play_hours_per_viewer " +
            " sum(play_count) play_times_per_viewer, " +
            " sum(viewer_count) play_viewer, " +
            " toHour(stt) AS hour  " +
            " FROM dws_interaction_chapter_video_play_window " +
            " WHERE toYYYYMMDD(stt) = #{date} " +
            " GROUP BY toHour(stt)")
    List<InteractionPlayHour> statsInteractionPlayHour(int date);


    @Select(
            "SELECT " +
//            " chapter_name, " +
            " sum(play_count) play_times_per_viewer, " +
            " sum(viewer_count) play_viewer " +
            " FROM dws_interaction_chapter_video_play_window " +
            " WHERE toYYYYMMDD(stt) = #{date} "
//            " and 'chapter_id' = '23349' " +
//            " GROUP BY chapter_id"
    )
    List<InteractionPlayTime> statsInteractionPlayTime(int date);
}
