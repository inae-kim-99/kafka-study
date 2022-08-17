package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KStreamJoinKTable {
    private static String APPLICATION_NAME = "order-join-application"; // 조인을 위한 카프카 스트림즈 처리임을 구분하기 위해 신규 애플리케이션 이름을 생성
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_TABLE = "address"; // 생성한 3개의 토픽을 사용하기 위해 String으로 선언한다.
    private static String ORDER_STREAM = "order";
    private static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> addressTable = builder.table(ADDRESS_TABLE); // address 토픽을 KTable로 가져온다.
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM); // order 토픽을 KStream으로 가져온다.

        orderStream.join(addressTable, // 조인을 위해 join() 메서드를 활용. 파라미터로 조인을 수행할 KTable 인스턴스를 넣는다.
                (order, address) -> order + " send to " + address) // 동일한 메시지 키를 가진 데이터를 찾는 경우 어떤 데이터를 만들지 정의.
                .to(ORDER_JOIN_STREAM); // order_join 토픽에 저장하기 위해 to() 싱크 프로세서를 사용한다.

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
