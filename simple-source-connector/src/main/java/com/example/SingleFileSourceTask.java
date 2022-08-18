package com.example;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleFileSourceTask extends SourceTask {

    private Logger logger = LoggerFactory.getLogger(SingleFileSourceTask.class);

    public final String FILENAME_FIELD = "filename"; // 파일 이름과 해당 파일을 읽은 지점을 오프셋 스토리지에 저장하기 위해
    public final String POSITION_FIELD = "position"; // filename과 position 값을 정의한다.

    private Map<String, String> fileNamePartition; // 오프셋 스토리지에 데이터를 저장하고 읽을 때는 Map 자료구조에 담은 데이터를 사용한다.
    private Map<String, Object> offset; // filename이 키, 커넥터가 읽은 파일 이름이 값으로 저장되어 사용된다.
    private String topic;
    private String file;
    private long position = -1; // 읽은 파일의 위치를 저장한다.
    // 커넥터가 최초로 실행될 때 오프셋 스토리지에서 마지막으로 읽은 파일의 위치를 position 변수에 전언하여 중복 적재되지 않도록 할 수 있다.

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            // Init variables
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props); // 커넥터 실행 시 받은 설정값을 사용한다.
            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);
            fileNamePartition = Collections.singletonMap(FILENAME_FIELD, file);
            offset = context.offsetStorageReader().offset(fileNamePartition);

            // get file offset from offsetStorageReader
            if (offset != null) {
                Object lastReadFileOffset = offset.get(POSITION_FIELD); // 오프셋 스토리지에서 현재 읽고자 하는 파일 정보를 가져온다.
                if (lastReadFileOffset != null) {
                    position = (Long) lastReadFileOffset; // 마지막으로 처리한 지점을 position 변수에 할당함으로서 데이터의 중복, 유실을 예방할 수 있다.
                }
            } else {
                position = 0; // 파일의 첫째 줄부터 처리하여 토픽으로 데이터를 보낸다.
            }
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException { // 태스크가 시작한 이후 지속적으로 데이터를 가져오기 위해 반복적으로 호출되는 메서드
        List<SourceRecord> results = new ArrayList<>();
        try {
            Thread.sleep(1000);

            List<String> lines = getLines(position); // 마지막으로 읽었던 지점 이후로 마지막까지 읽는다.

            if (lines.size() > 0) {
                lines.forEach(line -> {
                    Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FIELD, ++position);
                    SourceRecord sourceRecord = new SourceRecord(fileNamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                    results.add(sourceRecord);
                });
            }
            return results;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ConnectException(e.getMessage(), e);
        }
    }

    private List<String> getLines(long readLine) throws Exception {
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(readLine).collect(Collectors.toList());
    }

    @Override
    public void stop() {
    }
}
