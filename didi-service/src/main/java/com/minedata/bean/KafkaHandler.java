package com.minedata.bean;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaHandler {
    @Value("${topic}")
    private String topic;

    @Value("${partition}")
    private String partition;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public void createTopic() {
        System.out.println(topic + "created partition " + partition);
    }
}
