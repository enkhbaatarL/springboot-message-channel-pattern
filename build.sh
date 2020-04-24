#!/bin/bash
mvn install
docker build . -t quay.io/dawildone/spring-boot-message-pattern
docker push quay.io/dawildone/spring-boot-message-pattern