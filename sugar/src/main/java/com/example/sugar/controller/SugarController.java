package com.example.sugar.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sugar.bean.Kw;
import com.example.sugar.bean.TradeProvinceOrderStats;
import com.example.sugar.bean.TradeSourceOrderStats;
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