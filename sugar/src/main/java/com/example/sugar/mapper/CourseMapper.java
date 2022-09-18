package com.example.sugar.mapper;

import com.example.sugar.bean.TradeCourse;
import com.example.sugar.bean.TradeReview;
import com.example.sugar.bean.TradeSubject;
import com.example.sugar.bean.TradeCategory;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface CourseMapper {

    @Select("select course_name,\n" +
            "       sum(order_count)  order_count,\n" +
            "       count(user_id)    uu_count,\n" +
            "       sum(order_amount)   order_amount\n" +
            "from dws_trade_course_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by course_id, course_name")
    List<TradeCourse> tradeCourse(int date);

    @Select("select subject_name,\n" +
            "       sum(order_count)  order_count,\n" +
            "       count(user_id)    uu_count,\n" +
            "       sum(order_amount)   order_amount\n" +
            "from dws_trade_course_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by subject_id, subject_name")
    List<TradeSubject> tradeSubject(int date);

    @Select("select category_name,\n" +
            "       sum(order_count)  order_count,\n" +
            "       count(user_id)    uu_count,\n" +
            "       sum(order_amount)   order_amount\n" +
            "from dws_trade_course_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by category_id, category_name")
    List<TradeCategory> tradeCategory(int date);

    @Select("select course_name,    \n" +
            "                    round(avg_score,2) avg_score,    \n" +
            "                    uu_count,    \n" +
            "                    round(favor_user/uu_count*100,2)   favor_rate\n" +
            "       from\n" +
            "       (select course_name,    \n" +
            "                    sum( toInt32(review_stars) )/count(user_id)  avg_score,    \n" +
            "                    count(user_id)    uu_count \n" +
            "             from \n" +
            "       dws_interaction_review_window\n" +
            "       where toYYYYMMDD(stt) = 20220704    \n" +
            "       group by course_name) t1\n" +
            "       join\n" +
            "       (\n" +
            "       select \n" +
            "       course_name,\n" +
            "       count(user_id) favor_user\n" +
            "       from dws_interaction_review_window\n" +
            "       where review_stars='5'\n" +
            "       and toYYYYMMDD(stt) = 20220704\n" +
            "             group by course_name\n" +
            "       ) t2\n" +
            "       on t1.course_name = t2.course_name")
    List<TradeReview> tradeReview(int date);

}
