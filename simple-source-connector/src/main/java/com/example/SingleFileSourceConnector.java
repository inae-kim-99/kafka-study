package com.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleFileSourceConnector extends SourceConnector {

    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try {
            new SingleFileSourceConnectorConfig(props); // 커넥트에서 SingleFileSourceConnector 커넥터를 생성할 때 받은 설정 값들을 초기화한다.
        } catch (ConfigException e) { // 만약 필수 설정값이 빠져있다면 ConnectException을 발생시킨다.
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() { // 태스크의 클래스 이름을 지정한다.
        return SingleFileSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) { // 태스크가 2개 이상인 경우 태스크마다 다른 설정값을 줄 때 사용. 여기서는 동일한 설정값을 갖도록 함.
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() { // 커넥터에서 사용할 설정값을 지정
        return SingleFileSourceConnectorConfig.CONFIG;
    }

    @Override
    public String version() {
        return "1.0";
    }
}
