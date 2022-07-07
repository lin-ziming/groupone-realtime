package com.example.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sugar.bean.InteractionPlayTime;
import com.example.sugar.bean.Kw;
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

    @RequestMapping("/sugar/playHours")
    public String playHours(int date){
        List<InteractionPlayTime> interactionPlayTimes = tradeService.statsInteractionPlayTime(date);
        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        JSONArray series = new JSONArray();

        return result.toJSONString();
    }
}