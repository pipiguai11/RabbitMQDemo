package com.lhw.rabbitmq.consumers;

import com.lhw.rabbitmq.config.RabbitMQConfig;
import com.lhw.rabbitmq.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
@Slf4j
public class MsgConsumer {

    /**
     * 这里两个Fanout队列，因为没有设置key，而且注册到同一个交换机上了
     * 因此，只要Fanout交换机收到消息后，会同时往这两个队列中放进去一个一模一样的消息
     * 然后这两个对应的消费者都会自动的消费这个消息
     * @param message
     */
    @RabbitListener(
            bindings = {@QueueBinding(value = @Queue(value = RabbitMQConfig.FANOUT_QUEUE_NAME, durable = "true"),
                                    exchange = @Exchange(value = RabbitMQConfig.TEST_FANOUT_EXCHANGE, type = "fanout"))
                    })
    @RabbitHandler
    public void processFanoutMsg(Message message) {
        String msg = new String(message.getBody(), StandardCharsets.UTF_8);
        log.info("received Fanout message : " + msg);
    }

    @RabbitListener(
            bindings = {@QueueBinding(value = @Queue(value = RabbitMQConfig.FANOUT_QUEUE_NAME1, durable = "true"),
                                    exchange = @Exchange(value = RabbitMQConfig.TEST_FANOUT_EXCHANGE, type = "fanout"))
                    })
    @RabbitHandler
    public void processFanout1Msg(Message message) {
        String msg = new String(message.getBody(), StandardCharsets.UTF_8);
        log.info("received Fanout1 message : " + msg);
    }

    @RabbitListener(
            bindings = {@QueueBinding(value = @Queue(value = RabbitMQConfig.DIRECT_QUEUE_NAME, durable = "true"),
                                    exchange = @Exchange(value = RabbitMQConfig.TEST_DIRECT_EXCHANGE),
                                    key = RabbitMQConfig.DIRECT_ROUTINGKEY)
                    })
    @RabbitHandler
    public void processDirectMsg(Message message) {
        String msg = new String(message.getBody(), StandardCharsets.UTF_8);
        log.info("received Direct message : " + msg);
    }

    /**
     * 这里如果定义多个同一个队列的消费者，那会根据负载均衡规则，每个消费者消费一次
     * 因为每个消息只允许被消费一次，消费完之后会自动从队列中删除。
     * @param message
     */
    @RabbitListener(
            bindings = {@QueueBinding(value = @Queue(value = RabbitMQConfig.TOPIC_QUEUE_NAME, durable = "true"),
                                    exchange = @Exchange(value = RabbitMQConfig.TEST_TOPIC_EXCHANGE, type = "topic"),
                                    key = RabbitMQConfig.TOPIC_ROUTINGKEY)
                    })
    @RabbitHandler
    public void processTopicMsg(Message message) {
        String msg = new String(message.getBody(), StandardCharsets.UTF_8);
        log.info("received Topic message : " + msg);
    }

    @RabbitListener(
            bindings = {@QueueBinding(value = @Queue(value = RabbitMQConfig.TOPIC_QUEUE_NAME, durable = "true"),
                                    exchange = @Exchange(value = RabbitMQConfig.TEST_TOPIC_EXCHANGE, type = "topic"),
                                    key = RabbitMQConfig.TOPIC_ROUTINGKEY)
                    })
    @RabbitHandler
    public void processTopicMsg2(Message message) {
        String msg = new String(message.getBody(), StandardCharsets.UTF_8);
        log.info("received Topic2222 message : " + msg);
    }

    /**
     * 如下这个消费者，它和前面两个都是属于同一个交换机同一个队列的消费者
     *      但是这个消费者不能处理String和Byte数组的数据，只能处理Person对象的数据
     *      所以如果在消费时，根据负载均衡策略轮到这个消费者消费一些String和byte数组数据时，会报错
     * @param person
     */
    @RabbitListener(
            bindings = {@QueueBinding(value = @Queue(value = RabbitMQConfig.TOPIC_QUEUE_NAME, durable = "true"),
                    exchange = @Exchange(value = RabbitMQConfig.TEST_TOPIC_EXCHANGE, type = "topic"),
                    key = RabbitMQConfig.TOPIC_ROUTINGKEY)
            })
    @RabbitHandler
    public void processTopicMsgPerson(@Payload Person person) {
        System.out.println(person);
        log.info("received Topic2222 message : " + person);
    }

    /**
     * 如下这个消费者和上面两个消费者绑定的是同一个转换机但是不是同一个的队列
     * 也就是说，如果交换机收到消息，会往这个队列和上面两个消费者绑定的队列中同时存入一个消息待处理
     * 然后每个队列再根据负载均衡算法选择消费者去进行调度消费
     *
     *      这种绑定同一个转换机不同队列的方式用于实现群发功能，也就是发布订阅
     *          意思就是，同一个交换机上的所有队列都会接收到同一个消息，然后挑选一个消费者来消费队列中的消息
     *
//     * @param message
     */
    /*@RabbitListener(
            bindings = {@QueueBinding(value = @Queue(value = RabbitMQConfig.TOPIC_QUEUE_NAME2, durable = "true"),
                                    exchange = @Exchange(value = RabbitMQConfig.TEST_TOPIC_EXCHANGE, type = "topic"),
                                    key = RabbitMQConfig.TOPIC_ROUTINGKEY2)
                    })
    @RabbitHandler
    public void processTopicMsg3(Message message) {
        String msg = new String(message.getBody(), StandardCharsets.UTF_8);
        log.info("received Topic33333 message : " + msg);
    }*/

    @RabbitListener(
            bindings = {@QueueBinding(value = @Queue(value = RabbitMQConfig.TOPIC_QUEUE_NAME2, durable = "true"),
                    exchange = @Exchange(value = RabbitMQConfig.TEST_TOPIC_EXCHANGE, type = "topic"),
                    key = RabbitMQConfig.TOPIC_ROUTINGKEY2)
            })
    @RabbitHandler
    public void processTopicMsg3(@Payload String body, @Headers Map<String,Object> headers) {
        System.out.println(" body : " + body);
        System.out.println(" headers : " + headers);
    }

}
