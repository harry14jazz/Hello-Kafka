package com.company;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        // Create logger object
        final Logger logger = LoggerFactory.getLogger(Producer.class);

        // Create properties object for Producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // Create the ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<>("sample-topic-20220209","Learn Kafka at 20220211 - " + i);

            // Send Data - Asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.debug("\nRecived record metadata. \n" +
                                "Topic: " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + ", " +
                                "Offset: " + recordMetadata.offset() + " @ Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error Occured", e);
                    }
                }
            });
        }

        // Flush and Close Producer
        producer.flush();
        producer.close();
    }
}
