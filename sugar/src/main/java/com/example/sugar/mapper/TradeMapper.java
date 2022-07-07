package com.example.sugar.mapper;

import com.example.sugar.bean.InteractionPlayTime;
import com.example.sugar.bean.Kw;
import com.example.sugar.bean.TrafficVisitorTypeStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TradeMapper {

    @Select("SELECT \n" +
        "    keyword,\n" +
        "    sum(keyword_count)\n" +
        "FROM dws_traffic_source_keyword_page_view_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY keyword")
    List<Kw> statsKw(int date);

    @Select("SELECT \n" +
        "    is_new,\n" +
        "    sum(uv_ct) AS uv_ct,\n" +
        "    sum(pv_ct) AS pv_ct,\n" +
        "    sum(sv_ct) AS sv_ct,\n" +
        "    sum(uj_ct) AS uj_ct,\n" +
        "    sum(dur_sum) AS dur_sum\n" +
        "FROM dws_traffic_rc_vc_ch_ar_is_new_page_view_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY is_new")
    List<TrafficVisitorTypeStats> statsTrafficVisitorTypeStats(int date);



    @Select("SELECT " +
        " chapter_name , " +
        " sum(play_sec_sum)/60/60 play_hours, " +
        " sum(avg_sec_per_viewer)/60/60 play_hours_per_viewer " +
        " FROM dws_interaction_chapter_video_play_window " +
        " WHERE toYYYYMMDD(stt) = #{date} and chapter_id = 1000" +
        " GROUP BY chapter_name")
    List<InteractionPlayTime> statsInteractionPlayTime(int date);
}
