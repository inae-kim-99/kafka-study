package com.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {
    private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception != null) {
            logger.error(exception.getMessage(), exception);
        } else {
            logger.info(recordMetadata.toString());
        }
    }
}
