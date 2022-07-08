package com.example.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sugar.bean.*;
import com.example.sugar.service.TrafficSourceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/sugar/traffic")
public class TrafficController {

    // 自动装载渠道流量统计服务实现类
    @Autowired
    private TrafficSourceStatsService trafficSourceStatsService;

	// 1. 独立访客请求拦截方法
    @RequestMapping("/uvCt")
    public String getUvCt(int date) {

        List<TrafficUvCt> trafficUvCtList = trafficSourceStatsService.getUvCt(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        JSONArray data1 = new JSONArray();
        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            categories.add(trafficUvCt.getSourceSite());
            data1.add(trafficUvCt.getUvCt());
        }
        data.put("categories", categories);

        JSONArray series = new JSONArray();
        JSONObject one = new JSONObject();

        one.put("name", "独立访客数");
        one.put("data", data1);

        series.add(one);

        data.put("series",series);

        result.put("data", data);
        return result.toJSONString();
    }

    // 2. 会话数请求拦截方法
    @RequestMapping("/svCt")
    public String getPvCt(int date) {
        List<TrafficSvCt> svCtList = trafficSourceStatsService.getSvCt(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        JSONArray data1 = new JSONArray();

        for (TrafficSvCt svCt : svCtList) {
            categories.add(svCt.getSourceSite());
            data1.add(svCt.getSvCt());
        }

        data.put("categories", categories);

        JSONArray series = new JSONArray();
        JSONObject one = new JSONObject();
        series.add(one);

        one.put("name", "会话数");
        one.put("data", data1);

        data.put("series",series);

        result.put("data", data);
        return result.toJSONString();
    }

    // 3. 各会话浏览页面数请求拦截方法
    @RequestMapping("/pvPerSession")
    public String getPvPerSession(int date) {

        List<TrafficPvPerSession> list = trafficSourceStatsService.getPvPerSession(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        JSONArray data1 = new JSONArray();
        for (TrafficPvPerSession pvPerSession : list) {
            categories.add(pvPerSession.getSourceSite());
            data1.add(pvPerSession.getPvPerSession());
        }

        data.put("categories", categories);

        JSONArray series = new JSONArray();
        JSONObject one = new JSONObject();

        one.put("name", "会话平均页面浏览数");
        one.put("data", data1);

        series.add(one);

        data.put("series",series);

        result.put("data", data);
        return result.toJSONString();
    }

    // 4. 各会话累计访问时长请求拦截方法
    @RequestMapping("/durPerSession")
    public String getDurPerSession(int date) {

        List<TrafficDurPerSession> list = trafficSourceStatsService.getDurPerSession(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        JSONArray data1 = new JSONArray();
        for (TrafficDurPerSession durPerSession : list) {
            categories.add(durPerSession.getSourceSite());
            data1.add(durPerSession.getDurPerSession());
        }

        data.put("categories", categories);

        JSONArray series = new JSONArray();
        JSONObject one = new JSONObject();

        one.put("name", "会话平均页面访问时长");
        one.put("data", data1);

        series.add(one);

        data.put("series",series);

        result.put("data", data);
        return result.toJSONString();
    }

    @RequestMapping("/ujRate")
    public String getUjRate(int date) {

        List<TrafficUjRate> list = trafficSourceStatsService.getUjRate(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        JSONArray data1 = new JSONArray();

        for (TrafficUjRate ujRate : list) {
            categories.add(ujRate.getSourceSite());
            data1.add(ujRate.getUjRate());
        }

        data.put("categories", categories);

        JSONArray series = new JSONArray();
        JSONObject one = new JSONObject();

        one.put("name", "跳出率");
        one.put("data", data1);

        series.add(one);

        data.put("series",series);

        result.put("data", data);
        return result.toJSONString();
    }


}