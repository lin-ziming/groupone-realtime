package com.example.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sugar.bean.InteractionPlayHour;
import com.example.sugar.bean.InteractionPlayTime;
import com.example.sugar.bean.Kw;
import com.example.sugar.service.InteractionService;
import com.example.sugar.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
public class SugarController {

    // 会自动创建这个类的对象
    @Autowired
    TradeService tradeService;

    @Autowired
    InteractionService interactionService;

    @RequestMapping("/sugar/gmv")
    public String gmv(int date) {
        System.out.println(date);

        return "ok";
    }

    @RequestMapping("/sugar/kw")
    public String kw(int date) {
        System.out.println(date);

        List<Kw> list = tradeService.statsKw(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONArray data = new JSONArray();

        for (Kw kw : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", kw.getKeyword());
            obj.put("value", kw.getScore());

            data.add(obj);
        }

        result.put("data", data);
        return result.toJSONString();
    }

    //全章节当日播放时长和平均播放时长
    @RequestMapping("/sugar/playHours")
    public String playHours(int date) {
        System.out.println(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONArray categories = new JSONArray();
        for (int i = 0; i < 24; i++) {
            categories.add(i);
        }
        JSONArray series = new JSONArray();

        //series sumHours
        JSONObject sumHours = new JSONObject();
        sumHours.put("name", "总时长");
        sumHours.put("yAxisIndex", 0);
        JSONArray sumHoursData = new JSONArray();
        sumHours.put("data", sumHoursData);
        sumHours.put("unit", "H");
        series.add(sumHours);

        //series
        JSONObject avgHours = new JSONObject();
        avgHours.put("name", "人均时长");
        avgHours.put("yAxisIndex", 1);
        JSONArray avgHoursData = new JSONArray();
        avgHours.put("data", avgHoursData);
        avgHours.put("unit", "H");
        series.add(avgHours);

        List<InteractionPlayHour> list = interactionService.statsInteractionPlayHour(date);

        Map<Integer, InteractionPlayHour> map = new HashMap<>();
        for (InteractionPlayHour playHour : list) {
            map.put(playHour.getHour(), playHour);
        }

        for (int hour = 0; hour < 24; hour++) {
            InteractionPlayHour playTime = map.getOrDefault(hour, new InteractionPlayHour(0D, 0D, hour));
            sumHoursData.add(playTime.getPlay_sec_sum());
            avgHoursData.add(playTime.getAvg_sec_per_viewer());
        }

        JSONObject data = new JSONObject();
        data.put("categories", categories);
        data.put("series", series);
        data.put("yUnit", "小时");

        result.put("data", data);
        return result.toJSONString();

    }

    //全章节当日播放数和观众数
    @RequestMapping("/sugar/playTimes")
    public String playTimes(int date){
        System.out.println(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");


        JSONArray data = new JSONArray();

        JSONObject playTimesCount = new JSONObject();
        JSONObject playViewersCount = new JSONObject();

        playTimesCount.put("name", "播放次数");
        playTimesCount.put("desc", "播放次数指当日被用户观看的次数");
        playTimesCount.put("rate_level", "green");

        playViewersCount.put("name", "观众数");
        playViewersCount.put("desc", "观众数指当日已经有多少用户观看过");
        playViewersCount.put("rate_level", "custom");
        playViewersCount.put("color", "#f05050");


        List<InteractionPlayTime> playTimes = interactionService.statsInteractionPlayTime(date);

        playTimesCount.put("value", playTimes.get(0).getPlay_count());
        playViewersCount.put("value", playTimes.get(0).getViewer_count());


        data.add(playTimesCount);
        data.add(playViewersCount);

        result.put("data", data);

        return result.toJSONString();

    }
}