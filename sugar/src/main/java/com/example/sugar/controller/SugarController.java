package com.example.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sugar.bean.Kw;
import com.example.sugar.bean.TrafficVisitorTypeStats;
import com.example.sugar.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController
public class SugarController {

    // 会自动创建这个类的对象
    @Autowired
    TradeService tradeService;

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
        result.put("status",0);
        result.put("msg","");

        JSONArray data = new JSONArray();

        for (Kw kw : list) {
            JSONObject obj = new JSONObject();
            obj.put("name",kw.getKeyword());
            obj.put("value",kw.getScore());

            data.add(obj);
        }

        result.put("data",data);
        return result.toJSONString();
    }
}