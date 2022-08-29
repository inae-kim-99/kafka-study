package com.pipeline;

import com.google.gson.Gson;
import com.pipeline.config.ElasticSearchSinkConnectorConfig;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import java.util.Map;

public class ElasticSearchSinkTask extends SinkTask {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkTask.class);

    private ElasticSearchSinkConnectorConfig config;
    private RestHighLevelClient esClient;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            config = new ElasticSearchSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException(e.getMessage());
        }

        esClient = new RestHighLevelClient( // 엘라스틱서치에 적재하기 위해 RestHighLevelClient를 생성. (사용자가 입력한 호스트와 포트를 기반으로)
                RestClient.builder(new HttpHost(config.getString(config.ES_CLUSTER_HOST),
                        config.getInt(config.ES_CLUSTER_PORT))));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() > 0) { // 레코드가 1개 이상인 경우
            BulkRequest bulkRequest = new BulkRequest(); // 엘라스틱 서치로 전송하기 위한 BulkRequest를 생성. 1개 이상의 데이터를 묶음
            for (SinkRecord record : records) {
                Gson gson = new Gson();
                Map map = gson.fromJson(record.value().toString(), Map.class); // Map 타입의 데이터로 만든다.
                bulkRequest.add(new IndexRequest(config.getString(config.ES_INDEX)) // 레코드들을 BultRequest에 추가.
                        .source(map, XContentType.JSON));
                logger.info("record : {}", record.value());

            }

            esClient.bulkAsync( // BulkRequest에 담은 데이터들을 bulkAsync() 메서드로 전송. 비동기로 결과를 받아서 확인할 수 있음
                    bulkRequest,
                    RequestOptions.DEFAULT,
                    new ActionListener<BulkResponse>() { // 전송, 실패 여부에 따라 로그를 남긴다.
                        @Override
                        public void onResponse(BulkResponse bulkItemResponses) {
                            if (bulkItemResponses.hasFailures()) {
                                logger.error(bulkItemResponses.buildFailureMessage());
                            } else {
                                logger.info("bulk save success");
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    });
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets){ // 일정 주기마다 호출됨. put 메서드에서 데이터를 전송하므로 추가로 작성 X
        logger.info("flush");
    }

    @Override
    public void stop() { // 커넥터가 종료될 경우 esClient 변수를 안전하게 종료한다.
        try {
            esClient.close();
        } catch (IOException e) {
            logger.info(e.getMessage(), e);
        }
    }
}
