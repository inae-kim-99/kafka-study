package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleFileSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) { // 커넥트에서 SingleFileSinkConnector 커넥터를 생성할 때 받은 설정값들을 초기화함
        this.configProperties = props;
        try {
            new SingleFileSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SingleFileSinkTask.class; // 태스크 클래스 이름을 지정
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) { // 태스크가 2개 이상인 경우 태스크마다 다른 설정값을 줄 때 사용. 이 경우는 동일한 설정값을 지정함.
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
    public ConfigDef config() {
        return SingleFileSinkConnectorConfig.CONFIG; // 커넥터에서 사용할 설정값을 지정
    }

    @Override
    public String version() {
        return "1.0";
    }
}
