package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KStreamJoinGlobalKTable {


    private static String APPLICATION_NAME = "global-table-join-application"; // GlobalKTable을 사용하는 스트림즈 애플리케이션을 선언하기 위해 새로운 애플리케이션 아이디를 선언
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_GLOBAL_TABLE = "address_v2"; // 2개의 파티션으로 이루어진 address_v2, 3개의 파티션으로 이루어진 order 토픽 정의
    private static String ORDER_STREAM = "order";
    private static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<String, String> addressGlobalTable = builder.globalTable(ADDRESS_GLOBAL_TABLE);
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        orderStream.join(addressGlobalTable, // GlobalKTable 조인
                (orderKey, orderValue) -> orderKey, // GlobalKTable은 레코드 매칭 시 메시지 키, 메시지 값 둘 다 사용 가능. 여기서는 메시지 키와 매칭하도록 설정
                (order, address) -> order + " send to " + address) // 주문한 물품과 주소를 조합
                .to(ORDER_JOIN_STREAM); // order_join 토픽에 저장

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }



}
