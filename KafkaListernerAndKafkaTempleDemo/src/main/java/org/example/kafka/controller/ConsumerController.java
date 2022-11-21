package org.example.kafka.controller;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConsumerController {

    @KafkaListener(topics = "Demo.FirstTopic", groupId = "group-1")
    @GetMapping(value = "/getConsumerMsg")
    public String consumeMessages(String message){
        String receivedMsg="Messages received from kafka -"+message;
       System.out.println(receivedMsg);
       return receivedMsg;
    }
}
