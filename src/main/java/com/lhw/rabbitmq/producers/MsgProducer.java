package com.lhw.rabbitmq.producers;

import com.lhw.rabbitmq.config.RabbitMQConfig;
import com.lhw.rabbitmq.model.Person;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MsgProducer {

    private RabbitTemplate rabbitTemplate;

    /**
     * 自动注入RabbitTemplate
     *      设置消息发送者的MessageConverter为Jackson2JsonMessageConverter
     *      同时消息消费者也需要配置，使用一个配置文件配置SimpleRabbitListenerContainerFactory这个监听容器工厂
     * @param rabbitTemplate
     */
    MsgProducer(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
    }

    public void send2FanoutTestQueue(String massage){
        rabbitTemplate.convertAndSend(RabbitMQConfig.TEST_FANOUT_EXCHANGE,
                "", massage);
    }

    public void send2DirectTestQueue(String massage){
        rabbitTemplate.convertAndSend(RabbitMQConfig.TEST_DIRECT_EXCHANGE,
                RabbitMQConfig.DIRECT_ROUTINGKEY, massage);
    }

    public void send2TopicTestAQueue(String massage){
        rabbitTemplate.convertAndSend(RabbitMQConfig.TEST_TOPIC_EXCHANGE,
                "test.aaa", massage);
    }

    public void send2TopicTestBQueue(String massage) throws InterruptedException {
        for (int i = 0; i<10; i++){
            rabbitTemplate.convertAndSend(RabbitMQConfig.TEST_TOPIC_EXCHANGE,
                    "test.bbb", massage + i);
            Thread.sleep(500);
        }
    }

    public void send2TopicTestCQueue(String message){
        rabbitTemplate.convertAndSend(RabbitMQConfig.TEST_TOPIC_EXCHANGE,
                RabbitMQConfig.TOPIC_ROUTINGKEY2,message);
    }

    public void send2TopicTestPersonQueue(){
        Person p = new Person();
        p.setAddress("guangzhou");
        p.setName("lhw");
        for (int i = 0; i<10; i++){
            p.setAge(i);
            rabbitTemplate.convertAndSend(RabbitMQConfig.TEST_TOPIC_EXCHANGE,"test.ddd",p);
        }
    }

}
