package com.learning.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssingAndSeekDemo {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerAssingAndSeekDemo.class);
        String bootStrapServer = "localhost:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message

        //Assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offSetToReadFrom = 15;
        consumer.assign(Arrays.asList(topicPartition));

        //Seek
        consumer.seek(topicPartition, offSetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        while(keepOnReading) {  // just for tests purposes
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar++;
                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Partition: " + record.partition() + "OffSet: "+ record.offset());

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application!");
    }
}
