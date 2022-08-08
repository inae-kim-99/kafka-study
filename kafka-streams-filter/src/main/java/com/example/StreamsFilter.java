package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilter {

    private static String APPLICATION_NAME = "streams-filter-application"; // application.id 값을 기준으로 병렬처리하기 때문에 지정해야 한다.
    private static String BOOTSTRAP_SERVERS = "54.184.145.255:9092"; // 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보를 입력한다.
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // 메시지 키, 메시지 값의 직렬화/역직렬화할 방식을 지정
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder(); // 스트림 토폴로지를 정의
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);
        KStream<String, String> filteredStream = streamLog.filter( // 데이터를 필터링하는 filter() 메서드를 활용한다. 메시지 값의 길이가 5보다 큰 경우만 필터링하도록 한다.
                (key, value) -> value.length() > 5);
        filteredStream.to(STREAM_LOG_FILTER); // 필터링된 KStream을 stream_log_filter 토픽에 저장하도록 소스 프로세서를 작성한다.

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();


    }
}
