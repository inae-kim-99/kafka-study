package com.example;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class SingleFileSourceConnectorConfig extends AbstractConfig {

    public static final String DIR_FILE_NAME = "file"; // 어떤 파일을 읽을 것인지 정하기 위해 파일 위치와 파일 이름을 선언
    public static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tml/kafka.txt";
    public static final String DIR_FILE_NAME_DOC = "읽을 파일 경로와 이름";
    public static final String TOPIC_NAME = "topic"; // 어느 토픽으로 보낼 지에 대한 정보
    public static final String TOPIC_DEFAULT_VALUE = "test";
    public static final String TOPIC_DOC = "보낼 토픽 이름";

    public static ConfigDef CONFIG = new ConfigDef().define( // 커넥터에서 사용할 옵션값들에 대한 정의를 표현하는 데 사용됨.
            DIR_FILE_NAME,
            Type.STRING,
            DIR_FILE_NAME_DEFAULT_VALUE,
            Importance.HIGH, // 커넥터에서 반드시 사용자가 입력한 설정이 필요한 값인 경우 HIGH를 설정하면 된다.
            DIR_FILE_NAME_DOC)
            .define(TOPIC_NAME,
                    Type.STRING,
                    TOPIC_DEFAULT_VALUE,
                    Importance.HIGH,
                    TOPIC_DOC);

    public SingleFileSourceConnectorConfig(Map<String, String> props){
        super(CONFIG, props);
    }
}
