package com.pipeline;

import com.pipeline.consumer.ConsumerWorker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.codehaus.jackson.map.deser.std.StringDeserializer;
import org.eclipse.jetty.util.thread.ShutdownThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HdfsSinkApplication {
    private final static Logger logger = LoggerFactory.getLogger(HdfsSinkApplication.class);

    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092"; // 연동할 카프카 클러스터 정보, 토픽 이름, 컨슈머 그룹을 설정한다.
    private final static String TOPIC_NAME = "select-color";
    private final static String GROUP_ID = "color-hdfs-save-consumer-group";
    private final static int CONSUMER_COUNT = 3; // 생성할 스레드 개수. 변수로 활용함으로서 후추 컨슈머 스레드의 개수를 동적으로 운영할 수 있다.
    private final static List<ConsumerWorker> workers = new ArrayList<>();

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread()); // 안전한 컨슈머의 종료를 위해 선언

        Properties configs = new Properties(); // 컨슈머 설정을 선언
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ExecutorService executorService = Executors.newCachedThreadPool(); // 스테드 풀로 관리
        for (int i = 0; i < CONSUMER_COUNT;i++){ // CONSUMER_COUNT 개수 만큼 컨슈머 스레드를 생성
            workers.add(new ConsumerWorker(configs, TOPIC_NAME, i));
        }
        workers.forEach(executorService::execute); // 각 인스턴스들을 실행
    }

    static class ShutdownThread extends Thread {
        public void run(){
            logger.info("Shutdown hook");
            workers.forEach(ConsumerWorker::stopAndWakeup); // 셧다운 훅이 발생했을 경우 각 컨슈머 스레드에게 종료를 알림
        }
    }
}
