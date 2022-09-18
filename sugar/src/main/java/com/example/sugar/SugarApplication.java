package com.example.sugar;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.example.sugar.mapper")
public class SugarApplication {

    public static void main(String[] args) {
        SpringApplication.run(SugarApplication.class, args);
    }

}
