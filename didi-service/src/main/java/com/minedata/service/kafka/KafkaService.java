package com.minedata.service.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import com.minedata.bean.KafkaHandler;
import com.minedata.kafka.KafkaRPCService;

@Service("kafkaService")
@ComponentScan(basePackages = "com.minedata")
public class KafkaService implements KafkaRPCService {

    @Autowired
    private KafkaHandler kafkaHandler;

    @Override
    public String getAvailTopicList() {

        kafkaHandler.createTopic();
        return null;
    }

}
