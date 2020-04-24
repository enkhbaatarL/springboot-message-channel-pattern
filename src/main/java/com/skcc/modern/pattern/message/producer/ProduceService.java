package com.skcc.modern.pattern.message.producer;

public interface ProduceService {

    void publish(String topic, Object message);

    void publishAndSubscribe(String topic, String replyTo, Object message, String correlationId);
}
