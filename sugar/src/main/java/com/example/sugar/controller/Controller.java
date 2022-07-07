package com.example.sugar.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class Controller {

    @RequestMapping("/sugar/gmv")
    public String gmv(int date) {
        System.out.println(date);

        return "ok";
    }
}