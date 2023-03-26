package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;

public class ConsumerDemoWithShutdown {
  private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getName());

  public static void main(String[] args) {
    log.info(" welcome to  KAFKA Consumer");

    // create Consumer properties

    String groupId = "my-fourth-application";
    Properties properties = new Properties();

    // localhost
//    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

    // connect to conduktor playground
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
    properties.setProperty(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "");
    properties.setProperty("sasl.mechanism", "PLAIN");

    // consumer configs
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

    Thread mainThread = Thread.currentThread();

    // adding shutdown hook

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("detected a shutdown !!!!!!");
      log.info("lets exit consumer ");
      kafkaConsumer.wakeup();

      try {
        mainThread.join();
        log.info("COmpleted clean up for the consumer group !!!!");
      } catch(InterruptedException e) {
        e.printStackTrace();
      }
    }));

    try {

      kafkaConsumer.subscribe(Collections.singleton("demo_java"));
      while(true) {
        log.info("polling ");

        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
        for(ConsumerRecord<String, String> record : consumerRecords) {
          log.info("key: " + record.key() + "-" + "value: " + record.value());
          log.info("\n Partition: " + record.partition() + "-" + "Offset: " + record.offset());
        }
      }
    } catch(WakeupException exception) {
      log.info("WakeupException Esception raised");
    } catch(Exception e) {
      log.error("Unexpected Exception !!!");
    } finally {
      kafkaConsumer.close();
    }
  }
}
