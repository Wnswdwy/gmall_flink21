package com.wnswdwy.gmalllogger.comtroller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yycstart
 * @create 2021-02-09 12:14
 */
//@RestController = @Controller+@ResponseBody
@RestController
@Slf4j
public class LoggerController {

    @RequestMapping("testDemo")
    //    @ResponseBody
    public String test01() {
        System.out.println("11111");
        return "hello demo";
    }

    @RequestMapping("testDemo2")
    public String test02(@RequestParam("name") String nn,
                         @RequestParam("age") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String logStr) {

        //将数据落盘
        log.info(logStr);

        //将数据发送至Kafka ODS主题
        kafkaTemplate.send("ODS_BASE_LOG", logStr);

        return "success";
    }

}




