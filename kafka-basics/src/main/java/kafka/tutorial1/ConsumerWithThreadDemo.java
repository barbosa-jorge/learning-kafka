package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreadDemo {
    public static void main(String[] args) {
        new ConsumerWithThreadDemo().startConsumer();
    }

    private void startConsumer() {

        Logger logger = LoggerFactory.getLogger(ConsumerWithThreadDemo.class);
        CountDownLatch latch = new CountDownLatch(1); //latch for dealing with multiple threads
        String bootStrapServer = "localhost:9092";
        String groupId = "my_group_3";
        String topic = "first_topic";

        //Create runnable, thread and start it.
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(latch, bootStrapServer, groupId, topic);
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();

            // block until shutdown is completed
            try{
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted", e);
            } finally {
                logger.info("Application has exited!");
            }
        }));

        // block until other threads complete their tasks
        try{
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing!");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private String bootStrapServer;
        private String groupId;
        private String topic;

        public ConsumerRunnable(CountDownLatch latch, String bootStrapServer, String groupId, String topic) {
            this.latch = latch;
            this.groupId = groupId;
            this.bootStrapServer = bootStrapServer;
            this.topic = topic;
        }

        @Override
        public void run() {

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(topic));

            try{
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + "OffSet: "+ record.offset());
                    }
                }
            } catch(WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown(); // decrease countdown till zero, tell main code we are done with the consumer
            }
        }

        // Interrupt consumer.poll, throws WakeupException
        public void shutdown() {
            consumer.wakeup();
        }
    }
}
