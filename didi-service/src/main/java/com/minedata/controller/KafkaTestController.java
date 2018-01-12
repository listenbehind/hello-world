package com.minedata.controller;

import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.minedata.service.kafka.KafkaService;

/**
 * Created by Administrator on 2016/5/6 0006.
 */
@RestController
@EnableAutoConfiguration
@RequestMapping("/hushi")
public class KafkaTestController {

    // @Reference(protocol = "thrift2")
    // @Setter
    // private OrderService.Iface studentService;

    @Autowired
    private KafkaService kafkaService;

    @RequestMapping(value = "/test/{id}", method = RequestMethod.GET)
    public String student(@PathVariable("id") Long id) throws TException {
        return kafkaService.getAvailTopicList();
    }



}
