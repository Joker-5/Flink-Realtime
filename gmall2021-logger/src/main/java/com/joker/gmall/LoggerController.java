package com.joker.gmall;/*
 *项目名: gmall2021-parent
 *文件名: LoggerController
 *创建者: Joker
 *创建时间:2021/3/3 23:33
 *描述:

 */

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        log.info(jsonStr);
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }
}
