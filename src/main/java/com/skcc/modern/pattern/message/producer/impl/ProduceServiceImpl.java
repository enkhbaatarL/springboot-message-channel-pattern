package com.skcc.modern.pattern.message.producer.impl;

import com.skcc.modern.pattern.message.producer.ProduceService;
import com.skcc.modern.pattern.message.util.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class ProduceServiceImpl implements ProduceService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private String domainGroup;
    private String domainName;
    private RabbitTemplate rabbitTemplate;

    @Autowired
    public ProduceServiceImpl(@Value("${domain.group}") String domainGroup,
                              @Value("${domain.name}") String domainName,
                              RabbitTemplate rabbitTemplate) {
        this.domainGroup = domainGroup;
        this.domainName = domainName;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public void publish(String topic, Object message) {
        try {
            if (StringUtils.isEmpty(topic)) {
                throw new Exception("'topic' is required.");
            }
            String exchangeName = getExchangeName();
            Map<String, Object> header = new HashMap<>();
            Message msg = new Message().payload(message).header(header);
            rabbitTemplate.convertAndSend(exchangeName, topic, msg);
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void publishAndSubscribe(String topic, String replyTo, Object message, String correlationId) {
        try {
            if (StringUtils.isEmpty(topic)) {
                throw new Exception("'topic' is required.");
            }
            if (StringUtils.isEmpty(replyTo)) {
                throw new Exception("'replyTo' is required.");
            }
            String id = StringUtils.isEmpty(correlationId) ? UUID.randomUUID().toString() : correlationId;
            String exchangeName = getExchangeName();
//            CorrelationData correlationData = new CorrelationData();
//            correlationData.setId(id);
//            rabbitTemplate.setCorrelationKey(id);
            rabbitTemplate.setChannelTransacted(true);

            MessagePostProcessor messagePostProcessor = msg -> {
                Address address = new Address(getExchangeNameByTopic(replyTo), replyTo);
                msg.getMessageProperties().setReplyToAddress(address);
                msg.getMessageProperties().setCorrelationId(id);
                return msg;
            };
            rabbitTemplate.setBeforePublishPostProcessors(messagePostProcessor);

            Map<String, Object> header = new HashMap<>();
            header.put("correlationId", id);
            Message msg = new Message().payload(message).header(header);

            rabbitTemplate.convertAndSend(exchangeName, topic, msg);
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }


    private String getExchangeName() {
        String exchange = "";
        if (domainGroup != null && domainGroup.length() > 0) {
            exchange += domainGroup + ".";
        }
        if (domainName != null && domainName.length() > 0) {
            exchange += domainName + ".";
        }
        exchange += "publish";
        return exchange;
    }

    private String getExchangeNameByTopic(String topic) {
        String[] path = topic.split("\\.");
        String exchange = "";
        if (domainGroup != null && domainGroup.length() > 0) {
            exchange += domainGroup + ".";
        }
        if (path.length > 1) {
            if (path[1] != null && path[1].length() > 0) {
                exchange += path[1] + ".";
            }
        }
        exchange += "publish";
        return exchange;
    }
}
