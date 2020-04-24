package com.skcc.modern.pattern.message.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.PropertiesPropertySourceLoader;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileUrlResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.net.MalformedURLException;

// 패턴에서 제공하는 설정 정보의 우선순위가 낮아야 사용자가 변경할 설정값으로 적용이 가능하여 최하우 우선 순위로 조정
// 단, 패턴에서 제공하는 설정이 절대 불변이라면 Ordered.HIGHEST_PRECEDENCE 값으로 변경 전략 필요
@Order(Ordered.LOWEST_PRECEDENCE)
public class MessageChannelEnvironmentPostProcessor implements EnvironmentPostProcessor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        // K8S로 배포한 설정을 먼저 확인하고 로컬에 있는 설정을 확인하는 방식으로 구성
        try {
            Resource k8sPath = new FileUrlResource("/pattern/message_channel.yml");
            Resource localPath = new ClassPathResource("message_channel.yml");

            if (k8sPath.exists()) {
                logger.debug("Load k8s Path file : " + k8sPath.getFilename());
                addLast(environment, k8sPath);
            } else if (localPath.exists()) {
                logger.debug("Load Local Path file : " + localPath.getFilename());
                addLast(environment, localPath);
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void addLast(ConfigurableEnvironment environment, Resource resource) throws IOException {
        environment.getPropertySources().addLast(new PropertiesPropertySourceLoader().load("patternConfig", resource).get(0));
    }
}
