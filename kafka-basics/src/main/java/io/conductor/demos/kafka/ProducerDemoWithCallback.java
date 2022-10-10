package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am kafka Producer");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java", "hello world " + i);

            // send data asynchronous operation
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e == null) {
                        log.info("Received new metadata /\n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offsets: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }


        // flush data - synchronous
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
// https://www.youtube.com/watch?v=aKDWWICgfA0&ab_channel=JavaGuides
// C:\kafka_2.13-3.2.1>  .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
// C:\kafka_2.13-3.2.1>.\bin\windows\kafka-server-start.bat .\config\server.properties
// C:\kafka_2.13-3.2.1>.\bin\windows\kafka-topics.bat --create --topic demo_java --bootstrap-server localhost:9092