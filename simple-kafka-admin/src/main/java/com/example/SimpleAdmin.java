package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleAdmin {
    private final static Logger logger = LoggerFactory.getLogger(SimpleAdmin.class);
    private final static String BOOTSTRAP_SERVERS = "54.184.145.255:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        AdminClient admin = AdminClient.create(configs); // create() 메서드로 KafkaAdminClient를 받환받는다.


        // 카프카 클러스터의 브로커별 설정을 출력한다.
        logger.info("== Get broker information");
        try {
            for (Node node : admin.describeCluster().nodes().get()) {
                logger.info("node : {}", node);
                ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
                DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
                describeConfigs.all().get().forEach((broker, config) -> {
                    config.entries().forEach(configEntry -> logger.info(configEntry.name() +
                    "= " + configEntry.value()));
                });
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        // 토픽 정보를 조회한다.
        Map<String, TopicDescription> topicInformation = null;
        try {
            topicInformation = admin.describeTopics(Collections.singletonList("test")).all().get();
            logger.info("{}", topicInformation);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        // 어드민 API를 종료한다.
        admin.close();
    }
}
