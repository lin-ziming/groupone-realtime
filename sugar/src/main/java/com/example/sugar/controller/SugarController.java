package com.example.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.sugar.bean.Kw;
import com.example.sugar.bean.PageViewType;
import com.example.sugar.bean.UserChangeCtPerType;
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
}