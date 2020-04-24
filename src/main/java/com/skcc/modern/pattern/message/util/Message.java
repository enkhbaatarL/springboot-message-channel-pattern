package com.skcc.modern.pattern.message.util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Message {
    private String messageId;
    private Map<String, Object> header = new HashMap<>();
    private Object payload;
    private String correlationId = "";
    private Object correlationData = null;

    public Message() {
        this.messageId = UUID.randomUUID().toString();
    }

    public Message messageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public Message header(Map<String, Object> header) {
        this.header = header;
        return this;
    }

    public Message addHeader(String Key, Object value) {
        this.header.put(Key, value);
        return this;
    }

    public Message addHeaders(Map<String, Object> header) {
        this.header.putAll(header);
        return this;
    }

    public Message correlationId(String correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    public Message payload(Object payload) {
        this.payload = payload;
        return this;
    }

    public Message correlationData(Object correlationData) {
        this.correlationData = correlationData;
        return this;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public Map<String, Object> getHeader() {
        return header;
    }

    public void setHeader(Map<String, Object> header) {
        this.header = header;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public Object getCorrelationData() {
        return correlationData;
    }

    public void setCorrelationData(Object correlationData) {
        this.correlationData = correlationData;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageId='" + messageId + '\'' +
                ", header=" + header +
                ", payload=" + payload +
                ", correlationId='" + correlationId + '\'' +
                ", correlationData=" + correlationData +
                '}';
    }
}
