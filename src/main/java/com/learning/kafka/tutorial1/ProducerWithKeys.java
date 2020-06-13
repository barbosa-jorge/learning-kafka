package com.learning.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);
        String bootStrapServer = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Creating producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String key = "id_"+ i;
            String value = "Hello kafka" +i;

            /*
                Using a key, all messages using same key always go to the same partition.
            */

            // Creating producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Key: " + key);

            // send data - asynchronous
            producer.send(record, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("\n Topic: " + recordMetadata.topic() + "\n " +
                            "Partition: "+ recordMetadata.partition() +"\n " +
                            "OffSet: " + recordMetadata.offset() + "\n " +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("An error happend while producing: " + exception);
                }
            }).get(); // Blocking, turning to synchronous, just to test
        }
        // flush and close producer
        producer.close();
    }
}
