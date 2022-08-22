package com.pipeline;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController // 스프링부트의 REST API 컨트롤러로 사용함을 선언하기 위해 추가.
@CrossOrigin(origins = "*", allowedHeaders = "*")  // 다른 도메인에서도 호출할 수 있도록 추가.
public class ProduceController {

    private final Logger logger = LoggerFactory.getLogger(ProduceController.class);

    private final KafkaTemplate<String, String> kafkaTemplate; // 카프카 스프링의 kafkaTemplate 인스턴스 생성. 메시지 키, 메시지 값은 String 타입으로 설정.

    public ProduceController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/api/select") // GET 메서드 호출을 받기위해 추가.
    public void selectColor( // color와 user 파라미터를 받아서 변수에 넣는다.
            @RequestHeader("user-agent") String userAgentName,
            @RequestHeader(value = "color") String colorName,
            @RequestHeader(value = "user") String userName) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ"); // 최종 적재되는 데이터에 시간을 저장하기 위해 저장한다.
        Date now = new Date();
        Gson gson = new Gson();
        UserEventVO userEventVO = new UserEventVO(sdfDate.format(now), // VO 인스턴스 생성
                userAgentName, colorName, userName);
        String jsonColorLog = gson.toJson(userEventVO); // 자바 객체를 JSON포맷의 String 타입으로 변환.
        kafkaTemplate.send("select-color", jsonColorLog).addCallback( // select-color 토픽에 데이터를 전송. 메시지 값만 넣음
                new ListenableFutureCallback<SendResult<String, String>>() { // 전송 실패/성공을 로그로 남김
                    @Override
                    public void onFailure(Throwable ex) {
                        logger.error(ex.getMessage(), ex);
                    }

                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        logger.info(result.toString());
                    }
                }
        );
    }
}
