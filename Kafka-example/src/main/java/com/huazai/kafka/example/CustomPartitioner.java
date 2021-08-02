package com.huazai.kafka.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 自定义分区器
 * @author pyh
 * @date 2021/7/28 23:50
 */
public class CustomPartitioner implements Partitioner {
    /**
     *
     * @param topic 主题
     * @param key key值
     * @param keyBytes key值字节
     * @param value value值
     * @param valueBytes value值字节码
     * @param cluster 集群信息
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取指定主题可用分区集合
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic("test_topic");
        return partitionInfos.size() - 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
