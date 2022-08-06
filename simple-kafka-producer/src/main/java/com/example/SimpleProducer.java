package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test"; // 생성한 레코드를 전송하기 위해 전송하고자 하는 토픽을 알고 있어야 함.
    private final static String BOOTSTRAP_SERVERS = "54.184.145.255:9092"; // 전송하고자 하는 카프카 클러스터 서버의 host와 IP를 지정.

    public static void main(String[] args) {

        Properties configs = new Properties(); // KafkaProducer 인스턴스를 생성하기 위한 프로듀서 옵션들을 key/value 값으로 선언한다.
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 메시지 키, 메시지 값을 직렬화하기 위한 직렬화 클래스를 선언.
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // string 객체를 전송하기 위해 string을 직렬화하는 클래스인 StringSerializer를 사용한다.
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class); // 커스텀 파티셔너를 지정한 경우 사용자 생성 파티셔너로 설정하여 인스턴스를 생성한다.

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs); // Properties를 KafkaProducer의 생성 파라미터로 추가하여 인스턴스를 생성한다.

        String messageValue = "testMessage"; // 메시지 값을 선언한다.

        // 카프카 브로커로 데이터를 보내기 위해 ProducerRecord를 생성한다. 메시지 키는 따로 선언하지 않아 null로 설정되어 전송된다.
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        // 메시지 키가 포함된 레코드를 전송하는 경우
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "Pangyo", "23");

        producer.send(record); // 즉각적인 전송이 아닌 파라미터로 들어간 record를 프로듀서 내부에 가지고 있다가 배치 형태로 묶어 브로커에 전송한다.
        logger.info("{}", record);
        producer.flush(); // 프로듀서 내부 버퍼에 가지고 있던 레코드 배치를 브로커로 전송한다.
        producer.close(); // producer 인스턴스의 리소스들을 안전하게 종료한다.
    }
}
