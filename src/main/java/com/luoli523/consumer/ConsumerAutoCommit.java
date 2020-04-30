package com.luoli523.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

// This example demonstrates a simple usage of Kafka's consumer api
// that relies on automatic offset committing.
public class ConsumerAutoCommit {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092,localhoost:9093,localhost:9094,localhost:9095");
    props.setProperty("group.id", "test-consumer-group1");
    props.setProperty("auto.commit.enable", "true");
    props.setProperty("auto.commit.interval.ms", "1000");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Arrays.asList("test", "test2"));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
      }
    }
  }

}
