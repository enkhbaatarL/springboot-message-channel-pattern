package com.skcc.modern.pattern.message.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.skcc.modern.pattern.message.consumer.ConsumeService;
import com.skcc.modern.pattern.message.util.MessageListener;
import org.reflections.Reflections;
import org.reflections.scanners.*;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Set;

@Component
public class MessageListenerInitializr implements ApplicationRunner {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ConsumeService consumeService;

    private SimpleMessageListenerContainer simpleMessageListenerContainer;

    @Autowired
    public MessageListenerInitializr(ConsumeService consumeService,
                                     SimpleMessageListenerContainer simpleMessageListenerContainer) {
        this.consumeService = consumeService;
        this.simpleMessageListenerContainer = simpleMessageListenerContainer;
    }

    @Override
    public void run(ApplicationArguments args) {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage("com.skcc"))
                .setScanners(new MethodAnnotationsScanner())
                .filterInputsBy(new FilterBuilder().includePackage("com.skcc"))
        );
        Set<Method> method = reflections.getMethodsAnnotatedWith(MessageListener.class);
        method.forEach((m) -> {
            Annotation[] annotations = m.getAnnotations();
            for (Annotation annotation : annotations) {
                if (annotation instanceof MessageListener) {
                    MessageListener listener = (MessageListener) annotation;
                    String[] topics = listener.topics();
                    String replyTo = listener.replyTo();
                    if (topics.length > 0) {
                        if (StringUtils.isEmpty(replyTo)) {
                            consumeService.subscribe(topics, m);
                        } else {
                            consumeService.subscribeAndPublish(topics, m, replyTo);
                        }
                    }
                }
            }
        });
        simpleMessageListenerContainer.start();
    }
}
