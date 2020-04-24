package com.skcc.modern.pattern.message.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;

@Component
public class MessageUtils {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private RabbitAdmin rabbitAdmin;
    private MessageListenerAdapter messageListenerAdapter;
    private SimpleMessageListenerContainer simpleMessageListenerContainer;

    @Autowired
    public MessageUtils(RabbitAdmin rabbitAdmin,
                        MessageListenerAdapter messageListenerAdapter,
                        SimpleMessageListenerContainer simpleMessageListenerContainer) {
        this.rabbitAdmin = rabbitAdmin;
        this.messageListenerAdapter = messageListenerAdapter;
        this.simpleMessageListenerContainer = simpleMessageListenerContainer;
    }

    public void declareExchange(String exchangeName) {
        try {
            if (StringUtils.isEmpty(exchangeName)) {
                throw new Exception("The exchange name is required.");
            }
            rabbitAdmin.declareExchange(new TopicExchange(exchangeName, true, true, null));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void declareQueue(String queueName) {
        try {
            if (StringUtils.isEmpty(queueName)) {
                throw new Exception("The queue name is required.");
            }
            rabbitAdmin.declareQueue(new Queue(queueName, true, false, true, null));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void bindQueue(String exchange, String queue, String topic) {
        try {
            if (StringUtils.isEmpty(exchange) || StringUtils.isEmpty(queue)) {
                throw new Exception("The exchange and queue name are required.");
            }
            rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue(queue)).to(new TopicExchange(exchange)).with(topic));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void declareExchangeAndQueueAndBind(String exchangeName, String queueName, String topic, Method method) {
        try {
            if (StringUtils.isEmpty(exchangeName) || StringUtils.isEmpty(queueName)) {
                throw new Exception("The exchange and queue name are required.");
            }
            this.declareQueue(queueName);
            this.declareExchange(exchangeName);
            this.bindQueue(exchangeName, queueName, topic);
            Object object;
            Constructor<?>[] constructors = method.getDeclaringClass().getConstructors();

            if (constructors.length > 0) {
                Parameter[] parameters = constructors[0].getParameters();
                if (parameters.length > 0) {
                    Object[] params = Arrays.stream(parameters).map((p) -> null).toArray();
                    object = constructors[0].newInstance(params);
                } else {
                    object = method.getDeclaringClass().newInstance();
                }
            } else {
                object = method.getDeclaringClass().newInstance();
            }
            if (object != null) {
                String[] queues = simpleMessageListenerContainer.getQueueNames();
                messageListenerAdapter.setDelegate(object);
                messageListenerAdapter.setMandatoryPublish(true);
                messageListenerAdapter.addQueueOrTagToMethodName(queueName, method.getName());

                if (Arrays.stream(queues).noneMatch((q) -> q.equals(queueName))) {
                    simpleMessageListenerContainer.addQueueNames(queueName);
                }
                simpleMessageListenerContainer.setMessageListener(messageListenerAdapter);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public void declareExchangeAndQueueAndBindAndPublisher(String exchangeName, String queueName, String topic, String replyTo, Method method) {
        try {
            if (StringUtils.isEmpty(exchangeName) || StringUtils.isEmpty(queueName)) {
                throw new Exception("The exchange and queue name are required.");
            }
            this.declareQueue(queueName);
            this.declareExchange(exchangeName);
            this.bindQueue(exchangeName, queueName, topic);
            Object object;
            Constructor<?>[] constructors = method.getDeclaringClass().getConstructors();
            if (constructors.length > 0) {
                Parameter[] parameters = constructors[0].getParameters();
                if (parameters.length > 0) {
                    Object[] params = Arrays.stream(parameters).map((p) -> null).toArray();
                    object = constructors[0].newInstance(params);
                } else {
                    object = method.getDeclaringClass().newInstance();
                }
            } else {
                object = method.getDeclaringClass().newInstance();
            }
            if (object != null) {
                String[] queues = simpleMessageListenerContainer.getQueueNames();
                messageListenerAdapter.setDelegate(object);
                messageListenerAdapter.setMandatoryPublish(true);
                messageListenerAdapter.addQueueOrTagToMethodName(queueName, method.getName());
                final List<Message> receivedMessages = new ArrayList<>();
                MessagePostProcessor afterReceivePostProcessors = (message) -> {
                    ObjectMapper objectMapper = new ObjectMapper();
                    try {
                        Message receivedMessage = objectMapper.readValue(message.getBody(), Message.class);
                        if (receivedMessage != null) {
                            receivedMessages.add(receivedMessage);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return message;
                };
                MessagePostProcessor beforSendReplyPostProcessors = (message) -> {
                    ObjectMapper objectMapper = new ObjectMapper();
                    byte[] body = new byte[0];
                    try {
                        Message msg = objectMapper.readValue(message.getBody(), Message.class);
                        if (receivedMessages.size() > 0) {
                            Message receivedMessage = receivedMessages.get(0);
                            msg.setCorrelationId(receivedMessage.getHeader().get("correlationId").toString());
                            msg.setCorrelationData(receivedMessage);
                        }
                        body = objectMapper.writeValueAsBytes(msg);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return MessageBuilder
                            .withBody(body)
                            .andProperties(message.getMessageProperties())
                            .build();
                };
                simpleMessageListenerContainer.addAfterReceivePostProcessors(afterReceivePostProcessors);
                messageListenerAdapter.setBeforeSendReplyPostProcessors(beforSendReplyPostProcessors);
                if (Arrays.stream(queues).noneMatch((q) -> q.equals(queueName))) {
                    simpleMessageListenerContainer.addQueueNames(queueName);
                }
                simpleMessageListenerContainer.setMessageListener(messageListenerAdapter);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
