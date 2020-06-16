package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbacksDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallbacksDemo.class);
        String bootStrapServer = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Creating producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            // Creating producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello kafka " + i);

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
            });
        }
        // flush and close producer
        producer.close();
    }
}
