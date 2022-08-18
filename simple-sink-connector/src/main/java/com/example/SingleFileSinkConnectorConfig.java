package com.example;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import java.util.Map;

public class SingleFileSinkConnectorConfig extends AbstractConfig {

    public static final String DIR_FILE_NAME = "file"; // 토픽의 데이터를 저장할 파일 이름을 옵션값으로 받기 위해 선언한다.
    private static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DIR_FILE_NAME_DOC = "저장할 디렉토리와 파일 이름";

    public static ConfigDef CONFIG = new ConfigDef().define( // 커넥터에서 사용할 옵션값들에 대한 정의를 표현하는 데에 사용한다.
            DIR_FILE_NAME,
            Type.STRING,
            DIR_FILE_NAME_DEFAULT_VALUE,
            Importance.HIGH,
            DIR_FILE_NAME_DOC);

    public SingleFileSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
