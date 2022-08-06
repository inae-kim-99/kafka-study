package com.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null){ // 메시지 키를 지정하지 않은 경우 비정상적인 데이터로 간주하여 에러를 발생시킨다.
            throw new InvalidRecordException("Need message key");
        }
        if (((String)key).equals("Pangyo")){ // 메시지 키가 Pangyo인 경우 파티션 0번으로 지정되도록 한다.
            return 0;
        }

        // 해시값을 지정하여 특정 파티션에 배칭되도록 설정한다.
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() { }

    @Override
    public void configure(Map<String, ?> configs) { }
}
