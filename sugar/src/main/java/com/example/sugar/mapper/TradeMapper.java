package com.example.sugar.mapper;

import com.example.sugar.bean.*;
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

    @Select("select source,\n" +
            "\ttotal_amount amount,\n" +
            "\tuser_count userCount,\n" +
            "\torder_count orderCount,\n" +
            "\tuser_count / uv_count * 100 convertRate\n" +
            "from (\n" +
            "select source,\n" +
            "\tsum(amount) total_amount,\n" +
            "\tsum(count) user_count,\n" +
            "\tsum(times) order_count\n" +
            "from dws_trade_source_province_order_window\n" +
            "where toYYYYMMDD(stt)=#{date}\n" +
            "group by source\n" +
            ") so\n" +
            "join (\n" +
            "select source,\n" +
            "\tsum(count) uv_count\n" +
            "from dws_traffic_source_uv_window\n" +
            "where toYYYYMMDD(stt)=#{date}\n" +
            "group by source\n" +
            ") su\n" +
            "on so.source=su.source")
    List<TradeSourceOrderStats> getOrderInfoBySource(int date);

    @Select("select province,\n" +
            "\tsum(amount) amount,\n" +
            "\tsum(count) userCount,\n" +
            "\tsum(times) orderCount\n" +
            "from dws_trade_source_province_order_window\n" +
            "where toYYYYMMDD(stt)=#{date}\n" +
            "group by province")
    List<TradeProvinceOrderStats> getOrderInfoByProvince(int date);

    @Select("select sum(amount) " +
            "from dws_trade_source_province_order_window\n" +
            "where toYYYYMMDD(stt)=#{date}")
    Double getTotalAmount(int date);

    @Select("select sum(count) " +
            "from dws_trade_source_province_order_window\n" +
            "where toYYYYMMDD(stt)=#{date}")
    Long getUserCount(int date);

    @Select("select sum(times) " +
            "from dws_trade_source_province_order_window\n" +
            "where toYYYYMMDD(stt)=#{date}")
    Long getOrderCount(int date);


    //cxy
    @Select("SELECT \n" +
            "    'backCt' AS type,\n" +
            "    sum(back_ct) AS back_ct\n" +
            "FROM dws_user_user_active_and_back_window\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "SELECT \n" +
            "    'activeCt' AS type,\n" +
            "    sum(active_ct) AS active_ct\n" +
            "FROM dws_user_user_active_and_back_window\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "SELECT \n" +
            "    'newCt' AS type,\n" +
            "    sum(register_ct) AS register_ct\n" +
            "FROM dws_user_register_window\n" +
            "WHERE toYYYYMMDD(stt) = #{date}")
    List<UserChangeCtPerType> selectUserChangeCtPerType(int date);

    @Select("SELECT \n" +
            "    'home' AS page_id,\n" +
            "    sum(home_uv_ct) AS home_uv_ct\n" +
            "FROM dws_traffic_page_view_window\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "SELECT \n" +
            "    'course_detail' AS page_id,\n" +
            "    sum(course_detail_uv_ct) AS course_detail_uv_ct\n" +
            "FROM dws_traffic_page_view_window\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n")
    List<PageViewType> selectPageIdViewCtType(int date);
}
