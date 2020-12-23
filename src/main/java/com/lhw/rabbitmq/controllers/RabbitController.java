package com.lhw.rabbitmq.controllers;

import com.lhw.rabbitmq.producers.MsgProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@RequestMapping("/rabbit")
@Controller
@Slf4j
public class RabbitController {

    @Autowired
    private MsgProducer msgProducer;

    @GetMapping(value = "/sendFanout")
    @ResponseBody
    @Transactional(rollbackFor = Exception.class)
    public void sendMsg(){
        msgProducer.send2FanoutTestQueue("this is a test fanout message!");
    }

    @GetMapping(value = "/sendDirect")
    @ResponseBody
    @Transactional(rollbackFor = Exception.class)
    public void sendDirectMsg(){
        msgProducer.send2DirectTestQueue("this is a test direct message!");
    }

    @GetMapping(value = "/sendDirectA")
    @ResponseBody
    @Transactional(rollbackFor = Exception.class)
    public void sendTopicAMsg(){
        msgProducer.send2TopicTestAQueue("this is a test topic aaa message!");
    }

    @GetMapping(value = "/sendTopicB")
    @ResponseBody
    @Transactional(rollbackFor = Exception.class)
    public void sendTopicBMsg() throws InterruptedException {
        msgProducer.send2TopicTestBQueue("this is a test topic bbb message!");
    }

    @GetMapping(value = "/sendTopicC")
    @ResponseBody
    @Transactional(rollbackFor = Exception.class)
    public void sendTopicCMsg() {
        msgProducer.send2TopicTestCQueue("this is a test topic ccc message!");
    }

    @GetMapping(value = "/sendTopicD")
    @ResponseBody
    @Transactional(rollbackFor = Exception.class)
    public void sendTopicPersonMsg() {
        msgProducer.send2TopicTestPersonQueue();
    }

}
