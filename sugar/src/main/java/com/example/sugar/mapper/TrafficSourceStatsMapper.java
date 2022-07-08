package com.example.sugar.mapper;

import com.example.sugar.bean.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficSourceStatsMapper {
    // 1. 获取各来源独立访客数
    @Select("select sc, \n" +
            "   source_site, \n" +
            "   source_url, \n" +
            "   sum(uv_ct)   uv_ct \n" +
            "from dws_traffic_rc_vc_ch_ar_is_new_page_view_window \n" +
            "where toYYYYMMDD(stt) = #{date} \n" +
            "group by toYYYYMMDD(stt), sc,source_site,source_url \n" +
            "order by uv_ct desc")
    List<TrafficUvCt> selectUvCt(@Param("date")Integer date);

	// 2. 获取各来源会话数
    @Select("SELECT\n" +
            "    sc,\n" +
            "    source_site,\n" +
            "    source_url,\n" +
            "    sum(sv_ct) AS sv_ct\n" +
            "FROM dws_traffic_rc_vc_ch_ar_is_new_page_view_window\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY\n" +
            "    toYYYYMMDD(stt),\n" +
            "    sc,\n" +
            "    source_site,\n" +
            "    source_url\n" +
            "ORDER BY sv_ct DESC")
    List<TrafficSvCt> selectSvCt(@Param("date")Integer date);

    // 3. 获取各来源会话平均页面浏览数
    @Select("select sc,\n" +
            "   source_site," +
            "   source_url," +
            "       sum(pv_ct) / sum(sv_ct)   pv_per_session\n" +
            "from dws_traffic_rc_vc_ch_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), sc, source_site,source_url\n" +
            "order by pv_per_session desc")
    List<TrafficPvPerSession> selectPvPerSession(@Param("date")Integer date);

    // 4. 获取各来源会话平均页面访问时长
    @Select("select sc,\n" +
            "   source_site," +
            "   source_url," +
            "       sum(dur_sum) / sum(sv_ct) dur_per_session\n" +
            "from dws_traffic_rc_vc_ch_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), sc, source_site,source_url\n" +
            "order by dur_per_session desc")
    List<TrafficDurPerSession> selectDurPerSession(@Param("date")Integer date);

    // 5. 获取各来源跳出率
    @Select("select sc,\n" +
            "   source_site," +
            "   source_url," +
            "       sum(uj_ct) / sum(sv_ct)   uj_rate\n" +
            "from dws_traffic_rc_vc_ch_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), sc, source_site,source_url\n" +
            "order by uj_rate desc")
    List<TrafficUjRate> selectUjRate(@Param("date")Integer date);
}