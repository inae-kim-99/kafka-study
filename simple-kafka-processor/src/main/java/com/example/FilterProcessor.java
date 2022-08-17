package com.example;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class FilterProcessor implements Processor<String, String> {

    private ProcessorContext context; // 프로세서에 대한 정보를 담고 있음. 현재 스트림 처리 중인 토폴로지의 토픽 정보, 애플리케이션 아이디 조회 가능.

    @Override
    public void init(ProcessorContext context) { // 스트림 프로세서의 생성자
        this.context = context;
    }

    @Override
    public void process(String key, String value) { // 프로세싱 로직
        if (value.length() > 5){
            context.forward(key, value);
        }
        context.commit();
    }

    @Override
    public void close() {

    }
}
