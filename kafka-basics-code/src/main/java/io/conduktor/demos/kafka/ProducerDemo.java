package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;

public class ProducerDemo {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getName());

  public static void main(String[] args) {
    log.info(" welcome to  KAFKA Producer");

    // create producer properties

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

    // create Producer record
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hwllo world producer");

    // send data
    // async operation
    kafkaProducer.send(producerRecord);

    // flush and close producer
    kafkaProducer.flush(); // blocking
    kafkaProducer.close();
  }
}
