package org.example.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

@RestController
public class ProducerController {

    @Value(value = "${spring.kafka.topic}")
    private String topicName;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

   // @RequestMapping(value="/produce", method = RequestMethod.GET)
    @GetMapping(value="/produceMsg")
    public String produceKafkaMessages(){
       // kafkaTemplate.send(topicName, msg);
        String message= "Hi This is my first message from kafka producer";
        //kafkaTemplate.send(topicName,message);

        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topicName, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + message + "] due to : " + ex.getMessage());
            }
        });
        return "Messages has produced to kafka server, Please check logs to more details.";
    }

    @PostMapping(value="/produceCustomMsg")
    public String produceKafkaDynamicMessages(@RequestBody String message){
                kafkaTemplate.send(topicName,message);
        return "Messages has produced to kafka server, Please check logs to more details.";
    }


}
