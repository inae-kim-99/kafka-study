package com.pipeline;

import com.pipeline.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchSinkConnector extends SinkConnector {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkConnector.class);

    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) { // 커넥터가 최초로 실행될 때 실행되는 구문이다. 사용자로부터 설정값을 가져와 인스턴스를 생성한다.
        this.configProperties = props;
        try {
            new ElasticSearchSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException(e.getMessage());
        }
    }

    @Override
    public Class<? extends Task> taskClass() { // 커넥터를 실행했을 경우 태스크 역할을 할 클래스를 선언한다.
        return ElasticSearchSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) { // 태스크별로 다른 설정값을 부여할 경우 로직을 넣을 수 있다. 여기서는 모든 태스크에 동일한 설정값을 설정한다.
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() { // 커넥터 종료 시 로그를 남긴다.
        logger.info("Stop elasticsearch connector");
    }

    @Override
    public ConfigDef config() { // ElasticSearchSinkConnectorConfig에서 설정한 설정값을 리턴한다.
        return ElasticSearchSinkConnectorConfig.CONFIG;
    }

    @Override
    public String version() { // 커넥터 버전을 설정한다. 커넥터의 버전에 따른 변경사항을 확인하기 위해 버저닝을 할 때 필요하다.
        return "1.0";
    }
}
