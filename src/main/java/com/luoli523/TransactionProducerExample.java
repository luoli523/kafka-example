package com.luoli523;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionProducerExample {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094,localhost:9095");
    props.put("transactional.id", "my-transactional-id");

    Producer<String, String> producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());

    producer.initTransactions();

    try {
      producer.beginTransaction();
      for (int i = 0; i < 100; i++) {
        producer.send(new ProducerRecord<String, String>("exam-topic", Integer.toString(i), Integer.toString(i)));
      }
      producer.commitTransaction();
    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
      // We can't recover from these exceptions, so our only option is to close the producer and exit.
      producer.close();
    } catch (KafkaException e) {
      // For all other exceptions, just abort the transaction and try again.
      producer.abortTransaction();
    }
  }
}
