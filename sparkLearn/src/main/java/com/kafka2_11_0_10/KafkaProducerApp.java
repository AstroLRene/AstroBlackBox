package com.kafka2_11_0_10;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerApp {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String,String> producer = new KafkaProducer<>(props);

        for(int i = 0 ; i < 100 ; i ++){
            producer.send(new ProducerRecord<String,String>("astrotest",i+"",UUID.randomUUID()+""));
        }
        producer.close();
        System.out.println("数据发送完毕！");
    }
}
