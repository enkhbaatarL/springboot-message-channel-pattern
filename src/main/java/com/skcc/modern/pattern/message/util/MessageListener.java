package com.skcc.modern.pattern.message.util;

import org.springframework.messaging.handler.annotation.MessageMapping;

import java.lang.annotation.*;

/**
 * Customized Annotation for Listening Message
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface MessageListener {
    String[] topics() default {};

    String replyTo() default "";
}
