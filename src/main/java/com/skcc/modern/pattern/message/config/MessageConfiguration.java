package com.skcc.modern.pattern.message.config;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.InvocationResult;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.Expression;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

@Configuration
public class MessageConfiguration {

    @Value("${domain.group}")
    private String domainGroup;

    @Value("${domain.name}")
    private String domainName;

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    TopicExchange topicExchange() {
        String exchange = "";
        if (domainGroup != null && domainGroup.length() > 0) {
            exchange += domainGroup + ".";
        }
        if (domainName != null && domainName.length() > 0) {
            exchange += domainName + ".";
        }
        exchange += "publish";
        return new TopicExchange(exchange, true, true);
    }

    @Bean
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                  MessageConverter messageConverter) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(messageConverter);
        rabbitTemplate.setEncoding(String.valueOf(StandardCharsets.UTF_8));
        rabbitTemplate.setBeforePublishPostProcessors(message -> {
//            System.out.println("SEND body: " + new String(message.getBody()));
//            System.out.println("SEND properties: " + message.getMessageProperties().toString());
            return message;
        });
        return rabbitTemplate;
    }

    @Bean
    MessageConverter messageConverter() {
        Jackson2JsonMessageConverter messageConverter = new Jackson2JsonMessageConverter();
        messageConverter.getJavaTypeMapper().addTrustedPackages("*");
        messageConverter.setCreateMessageIds(true);
        messageConverter.setDefaultCharset("UTF-8");
        return messageConverter;
    }

    @Bean
    SimpleMessageListenerContainer simpleMessageListenerContainer(ConnectionFactory connectionFactory, RabbitAdmin rabbitAdmin) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setDefaultRequeueRejected(false);
        container.setMissingQueuesFatal(false);
        container.setAutoStartup(false);
        container.setAutoDeclare(true);
        container.setChannelTransacted(true);
        container.setAmqpAdmin(rabbitAdmin);
        return container;
    }

    @Bean
    MessageListenerAdapter messageListenerAdapter(MessageConverter messageConverter) {
        MessageListenerAdapter adapter = new MessageListenerAdapter() {
            @Override
            public void onMessage(Message message, Channel channel) throws Exception {
                Object delegateListener = this.getDelegate();
                if (delegateListener != this) {
                    if (delegateListener instanceof ChannelAwareMessageListener) {
                        ((ChannelAwareMessageListener)delegateListener).onMessage(message, channel);
                        return;
                    }

                    if (delegateListener instanceof MessageListener) {
                        ((MessageListener)delegateListener).onMessage(message);
                        return;
                    }
                }

                Object convertedMessage = this.extractMessage(message);
                String methodName = this.getListenerMethodName(message, convertedMessage);
                if (methodName == null) {
                    throw new AmqpIllegalStateException("No default listener method specified: Either specify a non-null value for the 'defaultListenerMethod' property or override the 'getListenerMethodName' method.");
                } else {
                    Object[] listenerArguments = this.buildListenerArguments(convertedMessage, channel, message);
                    Object result = this.invokeListenerMethod(methodName, listenerArguments, message);
                    if (result != null) {
                        this.handleResult(new InvocationResult(result, (Expression)null, (Type)null, (Object)null, (Method)null), message, channel);
                    } else {
                        this.logger.trace("No result object given - no result to handle");
                    }

                }
            }
        };
        adapter.setMessageConverter(messageConverter);
        adapter.setEncoding("UTF-8");

        return adapter;
    }
}
