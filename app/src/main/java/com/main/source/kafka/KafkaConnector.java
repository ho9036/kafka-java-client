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
    private final Properties properties;
    private final KafkaProducer<String, String> producer;
    private final HashMap<String, KafkaConsumer<String, String>> consumers;
    
    private KafkaConnector(Properties properties){
        this.properties = properties;
        this.producer = new KafkaProducer<>(properties);
        this.consumers = new HashMap<>();
    }

    public static KafkaConnector create(String domain){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", domain); // Kafka 서버 주소
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaConnector(properties);
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

    public void setConsumer(String topic){
        if (!consumers.containsKey(topic)){
            consumers.put(topic, new KafkaConsumer<>(properties));
        }
    }

    public KafkaConsumer<String,String> getConsumer(String topic){
        return consumers.get(topic);
    }

    public void dispose(){
        try (producer) {
            producer.flush();
        }

        consumers.clear();
    }
}