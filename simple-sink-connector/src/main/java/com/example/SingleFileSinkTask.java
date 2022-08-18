package com.example;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import java.util.Map;

public class SingleFileSinkTask extends SinkTask {

    private SingleFileSinkConnectorConfig config;
    private File file;
    private FileWriter fileWriter;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) { // 커넥터를 실행할 때 설정한 옵션을 토대로 리소스를 초기화한다. 파일 인스턴스를 생성한다.
        try {
            config = new SingleFileSinkConnectorConfig(props);
            file = new File(config.getString(config.DIR_FILE_NAME));
            fileWriter = new FileWriter(file, true);
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) { // 데이터를 저장한다. SinkRecord는 토픽의 레코드이며 토픽, 파티션, 타임스탬프 정보를 저장한다.
        try {
            for (SinkRecord record : records){
                fileWriter.write(record.value().toString() + "\n");
            }
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) { // 실질적으로 파일 시스템에 데이터를 저장하기 위해 flush 메서드를 호출한다.
        try {
            fileWriter.flush();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void stop() { // 태스크 종료 시 열고 있던 파일을 안전하게 닫는다.
        try {
            fileWriter.close();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }
}
