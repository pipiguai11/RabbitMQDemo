package com.lhw.rabbitmq.config;

import com.lhw.rabbitmq.model.Person;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

/**
 * 设置序列化和反序列化
 *      RabbitMQ提供Jackson2JsonMessageConverter来支持消息内容json的序列化和反序列化
 *      在这里手动设置一下消费者的MessageConverter消息转换器为Jackson2JsonMessageConverter即可
 *      然后还需要注意的时，消息发送者也需要设置为Jackson2JsonMessageConverter，否则会出错
 */
@Configuration
public class RabbitConverterConfig {

    @Bean
    public RabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory){
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
//        factory.setMessageConverter(new MessageConverter() {
//            @Override
//            public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
//                return null;
//            }
//
//            @Override
//            public Object fromMessage(Message message) throws MessageConversionException {
//                try(ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(message.getBody()))){
//                    return (Person)ois.readObject();
//                }catch (Exception e){
//                    e.printStackTrace();
//                    return null;
//                }
//            }
//        });
        factory.setMessageConverter(new Jackson2JsonMessageConverter());
        return factory;
    }

}
