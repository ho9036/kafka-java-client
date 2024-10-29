/*
 * This source file was generated by the Gradle 'init' task
 */
package kafka.sentence.streaming;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App {
    public static void main(String[] args) {
        ModeConsumer();
    }

    public static void ModeConsumer(){
         // Kafka Consumer 설정
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092,localhost:49092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "output-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 처음부터 읽기

        // Kafka Consumer 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        // `output-topic` 구독 설정
        consumer.subscribe(Collections.singletonList("output-topic"));

        try {
            // 메시지 수신 루프
            while (true) {
                // Kafka에서 메시지 폴링 (가져오기)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                // 각 레코드 처리
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed record with key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void ModeProducer(){
        List<String> sentences = FileReaderUtil.readSentencesFromResources("random_sentences.txt");

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:29092,localhost:39092,localhost:49092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        String[] keys = {"a01", "b02", "c03", "40d"};

        int index = 0;
        while (true) {  // 무한 루프
            try {
                Thread.sleep(1000);  // 1초 대기

                String key = keys[(int) (Math.random() * keys.length)];

                ProducerRecord<String, String> record = new ProducerRecord<>("input-topic", key, sentences.get(index));
                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    
                });

                index = (index + 1) % sentences.size();    // 인덱스 증가, 끝에 도달하면 다시 0으로
                
            } catch (InterruptedException e) {
                producer.close();
            }
        }
    }
}
