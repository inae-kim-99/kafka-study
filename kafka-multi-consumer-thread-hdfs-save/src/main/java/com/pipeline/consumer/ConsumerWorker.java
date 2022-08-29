package com.pipeline.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerWorker implements Runnable { // 컨슈머가 실행될 스레드를 정의하기 위해 Runnable 인터페이스로 구현

    private final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>(); // 전달 받은 데이터를 임시 저장하는 버퍼를 선언. (파티션 번호, 메시지값)
    private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>(); // 오프셋 값을 저장하고 파일 이름을 저장할 때 오프셋 번호를 붙이는 데에 사용

    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties prop, String topic, int number) {
        logger.info("Generate ConsumerWorker");
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number; // 스레드에 이름을 붙임으로서 로깅 시 편하게 스레드 번호를 확인할 수 있도록 함
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        consumer = new KafkaConsumer<>(prop); // 스레드를 생성하는 HdfsSinkApplication에서 설정한 컨슈머 설정을 가져와서 Kafka Consumer를 생성
        consumer.subscribe(Arrays.asList(topic)); // 토픽 구독
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // poll() 메서드로 데이터를 가져온다.

                for (ConsumerRecord<String, String> record : records) { // 데이터를 버퍼에 쌓는다.
                    addHdfsFileBuffer(record);
                }
                saveBufferToHdfsFile(consumer.assignment()); // 버퍼에 쌓인 데이터 개수가 기준을 넘을 경우 HDFS에 저장하는 로직을 수행하도록 한다
            }
        } catch (WakeupException e) {
            logger.warn("Wakeup consumer");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    private void addHdfsFileBuffer(ConsumerRecord<String, String> record){ // 레코드를 받아서 메시지 값을 버퍼에 넣는 코드이다.
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        if (buffer.size() == 1) // 버퍼 크기가 1이라면 버퍼의 가장 처음 오프셋이므로 currentFileOffset 변수에 넣는다.
            currentFileOffset.put(record.partition(), record.offset()); // 파티션 이름과 오프셋 번호를 함께 저장하여 이슈 발생시 참고할 수 있다.
    }

    private void saveBufferToHdfsFile(Set<TopicPartition> partitions){ // 버퍼의 데이터가 flush될 만큼 개수가 충족되었는지 확인하는 메서드를 호출한다.
        partitions.forEach(p -> checkFlushCount((p.partition())));
    }

    private void checkFlushCount(int partitionNo){ // 레코드가 일정 개수 이상이라면 HDFS에 적재한는 save() 메서드를 호출한다.
        if (bufferString.get(partitionNo) != null){
            if (bufferString.get(partitionNo).size() > FLUSH_RECORD_COUNT - 1){
                save(partitionNo);
            }
        }
    }

    private void save(int partitionNo){ // 실질적으로 HDFS 적재를 수행하는 메서드이다.
        if (bufferString.get(partitionNo).size() > 0)
            try {
                String fileName = "/data/color-" + partitionNo + "-" + currentFileOffset.get(partitionNo) + ".log"; // HDFS에 저장할 파일 이름
                Configuration configuration = new Configuration(); // HDFS 적재를 위한 설정
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem hdfsFileSystem = FileSystem.get(configuration); // 로컬 경로를 대상으로 FileSystem을 생성한다.
                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName)); // HDFS에 데이터를 파일로 저장하기 위한 인스턴스를 생성
                fileOutputStream.writeBytes(StringUtils.join(bufferString.get(partitionNo), "\n")); // fileOutputStream에 버퍼 데이터를 저장
                fileOutputStream.close();

                bufferString.put(partitionNo, new ArrayList<>()); // 적재 완료 후 버퍼 데이터를 초기화
            } catch (Exception e){
                logger.error(e.getMessage(), e); // 적재 중 Exception 발생한 경우 로그를 남긴다.
            }
    }

    private void saveRemainBufferToHdfsFile(){ // 버퍼에 남아있는 모든 데이터를 저장한다. (컨슈머 스레드 종료 시 호출)
        bufferString.forEach((partitionNo, v) -> this.save(partitionNo));
    }

    public void stopAndWakeup(){ // 셧다운 훅이 발생했을 때 안전한 종료를 위해 consumer에 wakeup() 메서드를 호출한다.
        logger.info("stopAndWakeup");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }
}
