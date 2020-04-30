package com.luoli523.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerManualCommit {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092,localhoost:9093,localhost:9094,localhost:9095");
    props.setProperty("group.id", "test-consumer-group2");
    props.setProperty("auto.commit.enable", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Arrays.asList("test", "test2"));

    final int minBatchSize = 5;
    List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        buffer.add(record);
      }

      if (buffer.size() > minBatchSize) {
        for (ConsumerRecord<String, String> record : buffer) {
          System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
        }
        consumer.commitSync();
        System.out.println("consumer synced");
        buffer.clear();
      }
    }
  }

}
