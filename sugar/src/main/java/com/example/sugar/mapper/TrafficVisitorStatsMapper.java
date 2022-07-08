package com.example.sugar.mapper;

import com.example.sugar.bean.TrafficVisitorStatsPerHour;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficVisitorStatsMapper {
    
// 分时流量数据查询
    @Select("SELECT\n" +
            "    t1.hr,\n" +
            "    sum(t1.uv_ct) AS uv_ct,\n" +
            "    sum(t1.pv_ct) AS pv_ct,\n" +
            "    sum(t1.pv_ct) / sum(t1.sv_ct) AS pv_per_session\n" +
            "FROM\n" +
            "(\n" +
            "    SELECT\n" +
            "        toHour(stt) AS hr,\n" +
            "        uv_ct,\n" +
            "        pv_ct,\n" +
            "        sv_ct\n" +
            "    FROM dws_traffic_rc_vc_ch_ar_is_new_page_view_window\n" +
            "    WHERE toYYYYMMDD(stt) = #{date}\n" +
            ") AS t1\n" +
            "GROUP BY t1.hr")
    List<TrafficVisitorStatsPerHour> selectVisitorStatsPerHr(Integer date);
}