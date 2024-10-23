package com.main.source.kafka;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerThread implements Runnable {
    private final KafkaConsumer<String, String> consumer;

    public ConsumerThread(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try (consumer) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Thread: %s, Key = %s, Value = %s, Partition = %d, Offset = %d%n",
                            Thread.currentThread().getName(), record.key(), record.value(), record.partition(), record.offset());
                }
            }
        }
    }
}