package com.joker.gmall;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.joker.gmall.mapper")
public class PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublisherApplication.class,args);
    }

}
