package com.nazjara;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "custom-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "some value");

        var consumer = new KafkaConsumer<String, String>(properties);

        try {
            final var mainThread = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Detecting a shutdown");
                // it will cause consumer to throw an exception to exit a loop
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    // do nothing
                }
            }));

            consumer.subscribe(List.of("demo_java"));

            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));

                for (var record : records) {
                    LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                    LOGGER.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("Wakeup exception. Expected when closing a consumer");
        } catch (Exception e) {
            LOGGER.error("Unexpected exception", e);
        } finally {
            consumer.close();
            LOGGER.info("Consumer is gracefully closed");
        }
    }
}
