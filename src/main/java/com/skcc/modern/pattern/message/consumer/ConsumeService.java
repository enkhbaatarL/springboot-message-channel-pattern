package com.skcc.modern.pattern.message.consumer;

import java.lang.reflect.Method;

public interface ConsumeService {

    void subscribe(String[] topics, Method method);

    void subscribeAndPublish(String[] topics, Method method, String reply);
}
