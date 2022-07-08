package com.example.sugar.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sugar.bean.InteractionPlayHour;
import com.example.sugar.bean.InteractionPlayTime;
import com.example.sugar.bean.Kw;
import com.example.sugar.bean.PageViewType;
import com.example.sugar.bean.UserChangeCtPerType;
import com.example.sugar.service.InteractionService;
import com.example.sugar.bean.TrafficVisitorTypeStats;
import com.example.sugar.bean.TradeProvinceOrderStats;
import com.example.sugar.bean.TradeSourceOrderStats;
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

    @RequestMapping("/sugar/visitorPerType")
    public String getVisitorPerType(int date) {
        System.out.println(date);

        List<TrafficVisitorTypeStats> visitorTypeStatsList  = tradeService.statsTrafficVisitorTypeStats(date);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");

        TrafficVisitorTypeStats newVisitorStats =null;
        TrafficVisitorTypeStats oldVisitorStats =null;
        for (TrafficVisitorTypeStats visitorStats  : visitorTypeStatsList) {
            System.out.println(visitorStats);
            if ("1".equals(visitorStats.getIsNew())) {
                // 新访客
                newVisitorStats = visitorStats;
            } else {
                // 老访客
                oldVisitorStats = visitorStats;
            }
        }
        //拼接json字符串
        String json = "{\"status\":0,\"data\":{\"total\":5," +
            "\"columns\":[" +
            "{\"name\":\"类别\",\"id\":\"type\"}," +
            "{\"name\":\"新访客\",\"id\":\"new\"}," +
            "{\"name\":\"老访客\",\"id\":\"old\"}]," +
            "\"rows\":[" +
            "{\"type\":\"访客数(人)\",\"new\":" + newVisitorStats.getUvCt() + ",\"old\":" + oldVisitorStats.getUvCt() + "}," +
            "{\"type\":\"总访问页面数(次)\",\"new\":" + newVisitorStats.getPvCt() + ",\"old\":" + oldVisitorStats.getPvCt() + "}," +
            "{\"type\":\"跳出率(%)\",\"new\":" + newVisitorStats.getUjRate() + ",\"old\":" + oldVisitorStats.getUjRate() + "}," +
            "{\"type\":\"平均在线时长(秒)\",\"new\":" + newVisitorStats.getAvgDurSum() + ",\"old\":" + oldVisitorStats.getAvgDurSum() + "}," +
            "{\"type\":\"平均访问页面数(人次)\",\"new\":" + newVisitorStats.getAvgPvCt() + ",\"old\":" + oldVisitorStats.getAvgPvCt() + "}]}}";

        return json;
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

    @RequestMapping("/sugar/groupOne/userCt")
    public String getUserChange(int date) {

        List<UserChangeCtPerType> userChangeCtList = tradeService.selectUserChangeCtPerType(date);
    //全章节当日播放时长和平均播放时长
    @RequestMapping("/sugar/playHours")
    public String playHours(int date) {
        System.out.println(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();

        JSONArray columns = new JSONArray();
        JSONObject column1 = new JSONObject();
        JSONObject column2 = new JSONObject();

        column1.put("name","变动类型");
        column1.put("id","type");

        column2.put("name","用户数");
        column2.put("id","user_ct");

        columns.add(column1);

        columns.add(column2);

        JSONArray rows = new JSONArray();

        for (UserChangeCtPerType userChangeCtPerType : userChangeCtList) {
            JSONObject obj = new JSONObject();
            obj.put("type",userChangeCtPerType.getType());
            obj.put("user_ct",userChangeCtPerType.getUserCt());

            rows.add(obj);
        }
        data.put("columns",columns);
        data.put("rows",rows);
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

    // 叙事折线图
    @RequestMapping("/sugar/trade/source")
    public String sourceOrder(int date) {
        System.out.println(date);

        List<TradeSourceOrderStats> list = tradeService.getOrderInfoBySource(date);
        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        JSONArray series = new JSONArray();

        JSONObject totalAmount = new JSONObject();
        totalAmount.put("name", "TotalAmount");
        totalAmount.put("type", "line");
        totalAmount.put("yAxisIndex", 0);

        JSONObject userCount = new JSONObject();
        userCount.put("name", "UserCount");
        userCount.put("type", "line");
        userCount.put("yAxisIndex", 0);

        JSONObject orderCount = new JSONObject();
        orderCount.put("name", "OrderCount");
        orderCount.put("type", "line");
        orderCount.put("yAxisIndex", 0);

        JSONObject convertRate = new JSONObject();
        convertRate.put("name", "ConvertRate");
        convertRate.put("type", "line");
        convertRate.put("yAxisIndex", 0);

        JSONArray totalAmountData = new JSONArray();
        JSONArray userCountData = new JSONArray();
        JSONArray orderCountData = new JSONArray();
        JSONArray convertRateData = new JSONArray();
        for (TradeSourceOrderStats v : list) {
            categories.add(v.getSource());
            totalAmountData.add(v.getAmount());
            userCountData.add(v.getUserCount());
            orderCountData.add(v.getOrderCount());
            convertRateData.add(v.getConvertRate());
        }
        totalAmount.put("data", totalAmountData);
        userCount.put("data", userCountData);
        orderCount.put("data", orderCountData);
        convertRate.put("data", convertRateData);
        series.add(totalAmount);
        series.add(userCount);
        series.add(orderCount);
        series.add(convertRate);

        data.put("series", series);
        data.put("categories",categories);

        result.put("data",data);

        return result.toJSONString();


    }

    @RequestMapping("/sugar/groupOne/pageCt")
    public String getPageViewCt(int date){
        List<PageViewType> pageViewList = tradeService.selectPageIdViewCtType(date);
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        JSONObject data = new JSONObject();

        JSONArray columns = new JSONArray();
        JSONObject column1 = new JSONObject();
        JSONObject column2 = new JSONObject();
        column1.put("name","网页类型");
        column1.put("id","page_id");

        column2.put("name","访问数");
        column2.put("id","view_ct");

        columns.add(column1);

        columns.add(column2);

        JSONArray rows = new JSONArray();
        for (PageViewType pageViewType : pageViewList) {
            JSONObject obj = new JSONObject();
            obj.put("page_id",pageViewType.getPageId());
            obj.put("view_ct",pageViewType.getUvCt());

            rows.add(obj);
        }
        data.put("columns",columns);
        data.put("rows",rows);
        result.put("data",data);

        return result.toJSONString();
    }

    // 叙事折线图
    @RequestMapping("/sugar/trade/province")
    public String provinceOrder(int date) {
        System.out.println(date);

        List<TradeProvinceOrderStats> list = tradeService.getOrderInfoByProvince(date);
        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        JSONArray series = new JSONArray();

        JSONObject totalAmount = new JSONObject();
        totalAmount.put("name", "TotalAmount");
        totalAmount.put("type", "line");
        totalAmount.put("yAxisIndex", 0);

        JSONObject userCount = new JSONObject();
        userCount.put("name", "UserCount");
        userCount.put("type", "line");
        userCount.put("yAxisIndex", 0);

        JSONObject orderCount = new JSONObject();
        orderCount.put("name", "OrderCount");
        orderCount.put("type", "line");
        orderCount.put("yAxisIndex", 0);

        JSONArray totalAmountData = new JSONArray();
        JSONArray userCountData = new JSONArray();
        JSONArray orderCountData = new JSONArray();
        for (TradeProvinceOrderStats v : list) {
            categories.add(v.getProvince());
            totalAmountData.add(v.getAmount());
            userCountData.add(v.getUserCount());
            orderCountData.add(v.getOrderCount());
        }
        totalAmount.put("data", totalAmountData);
        userCount.put("data", userCountData);
        orderCount.put("data", orderCountData);
        series.add(totalAmount);
        series.add(userCount);
        series.add(orderCount);

        data.put("series", series);
        data.put("categories",categories);

        result.put("data",data);
        return result.toJSONString();
    }

    // 数字翻牌器
    @RequestMapping("/sugar/trade/totalamount")
    public String totalAmount(int date) {
        System.out.println(date);
        Double totalAmount = tradeService.getTotalAmount(date);
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", totalAmount);
        return result.toJSONString();
    }

    // 数字翻牌器
    @RequestMapping("/sugar/trade/usercount")
    public String userCount(int date) {
        System.out.println(date);
        Long userCount = tradeService.getUserCount(date);
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", userCount);
        return result.toJSONString();
    }

    // 数字翻牌器
    @RequestMapping("/sugar/trade/ordercount")
    public String orderCount(int date) {
        System.out.println(date);
        Long orderCount = tradeService.getOrderCount(date);
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", orderCount);
        return result.toJSONString();
    }
}