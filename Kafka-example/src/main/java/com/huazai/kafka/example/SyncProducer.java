package com.huazai.kafka.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author pyh
 * @date 2021/7/28 23:43
 */
public class SyncProducer {
    private final static String TOPIC = "SYNC_TOPIC";

    private final static Integer COUNT = 100;

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        // kafka连接地址，多个地址用“,”隔开
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.132:9092,192.168.64.132:9093,192.168.64.132:9094");
        // 应答策略，all相当于-1
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 每个分区未发送消息总字节大小（单位：字节），超过设置的值就会提交数据到服务端
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 如果数据迟迟未达到 batch.size，sender 等待 linger.time 之后就会发送数据。
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // RecordAccumulator 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 序列化key所用到的类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 序列化value所用到的类
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < COUNT; i++) {
            producer.send(new ProducerRecord<>(TOPIC, "SYNC_TOPIC message：" + String.valueOf(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("分区："+ recordMetadata.partition() + "，偏移量：" + recordMetadata.offset());
                }
            });
        }
        Thread.sleep(5000);
        System.out.println("发送完毕！");
        producer.close();
    }
}
