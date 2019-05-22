FROM 		openjdk:8-jdk-alpine
MAINTAINER 	Uwe Geercken <uwe.geercken@web.de>

ADD 		kafka_ruleengine.sh /
ADD		run_kafka_ruleengine.sh /opt/ruleengine/
ADD		maven/lib/* /opt/ruleengine/lib/
ADD		kafka-ruleengine.properties /opt/ruleengine/config/
ADD		kafka-consumer.properties /opt/ruleengine/config/
ADD		kafka-producer.properties /opt/ruleengine/config/
ADD		run_kafka_ruleengine.sh /opt/ruleengine/
ADD		log4j.properties /opt/ruleengine/

RUN		apk add --no-cache openssl bash nano curl zip && \
		mkdir -p /opt/ruleengine/rules && \
		mkdir -p /opt/ruleengine/data && \
		adduser -D -h /opt/ruleengine -s /bin/sh engine && \
		chown -R engine:engine /opt/ruleengine && \
		chown engine:engine /kafka_ruleengine.sh

USER		engine
WORKDIR		/opt/ruleengine


