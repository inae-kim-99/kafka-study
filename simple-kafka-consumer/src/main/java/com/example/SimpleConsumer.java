package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;


public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test"; // 토픽 이름을 지정한다.
    private final static String BOOTSTRAP_SERVERS = "54.184.145.255:9092"; // 토픽의 데이터를 가져올 카프카 클러스터의 IP:PORT를 지정한다.
    private final static String GROUP_ID = "test-group"; // 컨슈머 그룹 이름을 선언한다. 컨슈머 그룹을 통해 컨슈머의 목적을 구분할 수 있다.

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // 프로듀서가 직렬화하여 전송한 데이터를 역직렬화하기 위해 역직렬화 클래스를 지정한다.
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs); // Properties로 지정한 카프카 컨슈머 옵션을 파라미터로 받아 KafkaConsumer 인스턴스를 생성한다.
        consumer.subscribe(Arrays.asList(TOPIC_NAME)); // 컨슈머에게 토픽을 할당하기 위해 subscribe 메서드를 사용한다.

        while (true) { // 무한루프를 통해 지속적으로 반복 호출한다.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // poll() 메서드를 호출하여 데이터를 가져와 처리한다.
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
            consumer.commitSync(); // poll() 메서드로 받은 가장 마지막 레코드의 오프셋을 기준으로 커밋한다.
        }

        // 개별 레코드 단위로 오프셋을 커핏하는 경우
//        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 명시적으로 오프셋 커밋을 수행하는 경우 해당 옵션을 false로 설정해야 한다.
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs); // Properties로 지정한 카프카 컨슈머 옵션을 파라미터로 받아 KafkaConsumer 인스턴스를 생성한다.
//        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener()); // 컨슈머에게 토픽을 할당하기 위해 subscribe 메서드를 사용한다.
//
//        while (true) { // 무한루프를 통해 지속적으로 반복 호출한다.
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // poll() 메서드를 호출하여 데이터를 가져와 처리한다.
//            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
//
//            for (ConsumerRecord<String, String> record : records) {
//                logger.info("{}", record);
//                currentOffset.put(
//                        new TopicPartition(record.topic(), record.partition()),
//                        new OffsetAndMetadata(record.offset() + 1, null));
//                consumer.commitSync(currentOffset);
//            }
//        }

//        // 비동기 오프셋 커밋을 사용하는 경우
//        consumer.commitAsync(new OffsetCommitCallback() {
//            @Override
//            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                if (exception != null)
//                    System.err.println("Commit Failed");
//                else
//                    System.out.println("Commit succeeded");
//                if (exception != null)
//                    logger.error("Commit Failed for offsets {}", offsets, exception);
//            }
//        }
    }

    private static class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned");
        }
    }
}
