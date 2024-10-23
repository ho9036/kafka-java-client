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
        //Kafka 클러스터의 주소(또는 도메인)를 지정
        producerProperties.put("bootstrap.servers", domain);
        //Kafka가 메시지 키를 직렬화하는 방법을 지정
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //메시지의 값을 직렬화하는 방법을 지정
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Properties consumerProperties = new Properties();
        //Kafka 클러스터의 주소(또는 도메인)를 지정
        consumerProperties.put("bootstrap.servers", domain);
        //Kafka에서 수신한 메시지의 키를 역직렬화(Deserialization)하는 방법 지정
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Kafka에서 수신한 메시지의 값을 역직렬화(Deserialization)하는 방법 지정
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConnector(producerProperties, consumerProperties);
    }

    // Producer를 통해 메시지 전송 (파티션을 고루 사용하고 싶으면 key명시를 해제하면됨. key명시를 해제하면 라운드 로빈 방식으로 메시지를 여러 파티션에 분배)
    public Boolean send(String topic, String key, String value){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        try {
            // 비동기 작업을 동기식으로 받아오기 위해 Future 사용
            Future<RecordMetadata> recordMetadata = producer.send(record);
            RecordMetadata result = recordMetadata.get();   

            System.out.println(result.timestamp() + ":::" + result.topic() + ":::" + key + ":::" + value);

            return true;
        } catch (InterruptedException | ExecutionException e) {

            System.out.println(topic + ":::" + key + ":::" + value + ":::" + e.getMessage());

            return false;
        }
    }

    // Consumer 생성 메서드
    // group.id를 여기서 넣어주는 이유는 모든 consumer가 서로 다른 쓰레드에서 진행하게 하기 위함
    // group.id가 같은 컨슈머들은 파티션이 서로 겹치지 않음. 따라서 메시지 읽음에 중복소비를 하지 않아 서로 다른 쓰레드로 동작하지 않음
    // group.id가 다르면 별도의 독립적인 그룹으로 간주되어 동일하지 않은 쓰레드를 부여받음
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