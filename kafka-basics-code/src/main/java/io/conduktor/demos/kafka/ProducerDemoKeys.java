package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;

public class ProducerDemoKeys {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

  public static void main(String[] args) {
    log.info(" welcome to  KAFKA Producer");

    Properties properties = new Properties();
    // localhost
//    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

    // connect to conduktor playground
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
    properties.setProperty(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "");
    properties.setProperty("sasl.mechanism", "PLAIN");

    // producer properties
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create producer client
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

    // send data
    // async operation
//    sticky partitioning demo
    // batching would happen and messages would land in the same partition to be more efficient
    // check     DefaultPartitioner class and  batch.size producer config for details

    /*
    From  DefaultPartitioner class:

    The default partitioning strategy:
    If a partition is specified in the record, use it
    If no partition is specified but a key is present choose a partition based on a hash of the key
    If no partition or key is present choose the sticky partition that changes when the batch is full.
    See KIP-480 for details about sticky partitioning.
     */

    for(int i = 0; i < 2; i++) {
      send(kafkaProducer);
    }

    // flush and close producer
    kafkaProducer.flush(); // blocking
    kafkaProducer.close();
  }

  private static void send(KafkaProducer kafkaProducer) {
    for(int i = 0; i < 10; i++) {
      String key = "id_" + i;
      // create Producer record
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", key, "hwllo world producer With callback count :" + i);

      kafkaProducer.send(producerRecord, (metadata, exception) -> {
        if(exception != null) {
          exception.printStackTrace();
          log.error(exception.toString());
        } else {
          log.info("Received new MeteData");
          log.info(" \n  Key :" + key +
             " \n  Partition :" + metadata.partition());
        }
      });
    }
  }
}
