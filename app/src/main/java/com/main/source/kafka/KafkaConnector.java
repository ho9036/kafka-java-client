package com.main.source.kafka;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaConnector{
    private final Properties consumerProperties;
    private final KafkaProducer<String, String> producer;
    private final HashMap<String, KafkaConsumer<String, String>> consumers;
    
    private KafkaConnector(Properties producerProperties, Properties consumerProperties){
        this.consumerProperties = consumerProperties;
        this.producer = new KafkaProducer<>(producerProperties);
        this.consumers = new HashMap<>();
    }

    public static KafkaConnector create(String domain){
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", domain); // Kafka 서버 주소
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", domain); // Kafka 서버 주소
        consumerProperties.put("group.id", "my-consumer-group");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConnector(producerProperties, consumerProperties);
    }

    public Boolean send(String topic, String key, String value){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        try {
            Future<RecordMetadata> recordMetadata = producer.send(record);
            RecordMetadata result = recordMetadata.get();   

            System.out.println(result.timestamp() + ":::" + result.topic() + ":::" + key + ":::" + value);

            return true;
        } catch (InterruptedException | ExecutionException e) {

            System.out.println(topic + ":::" + key + ":::" + value + ":::" + e.getMessage());

            return false;
        }
    }

    public void setConsumer(String topic, int partition){
        if (!consumers.containsKey(topic + "_" + partition)){
            consumerProperties.setProperty("group.id", topic + "_" + partition);
            consumers.put(topic + "_" + partition, new KafkaConsumer<>(consumerProperties));
        }
    }

    public KafkaConsumer<String,String> getConsumer(String topic, int partition){
        return consumers.get(topic + "_" + partition);
    }

    public void dispose(){
        try (producer) {
            producer.flush();
        }

        consumers.clear();
    }
}