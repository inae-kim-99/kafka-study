package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApplication {

    private static String APPLICATION_NAME = "streams-application"; // application.id 값을 기준으로 병렬처리하기 때문에 지정해야 한다.
    private static String BOOTSTRAP_SERVERS = "54.184.145.255:9092"; // 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보를 입력한다.
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // 메시지 키, 메시지 값의 직렬화/역직렬화할 방식을 지정
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder(); // 스트림 토폴로지를 정의
        KStream<String, String> streamLog = builder.stream(STREAM_LOG); // stream_log 토픽으로부터 KStream 객체를 만들기 위해 stream() 메서드 활용
        streamLog.to(STREAM_LOG_COPY); // stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송

        KafkaStreams streams = new KafkaStreams(builder.build(), props); // Kafkastreams 인스턴스를 생성하여 stream_log 토픽의 데이터를 stream_log_copy 토픽으로 전달한다.
        streams.start();
    }

}
