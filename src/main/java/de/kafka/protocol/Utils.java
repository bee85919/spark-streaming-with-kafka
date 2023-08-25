package de.kafka.protocol;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Utils {

    public static void printRecordMetadata(RecordMetadata metadata) {
        System.out.println(
                "Message sent successfully to " + metadata.topic() + metadata.partition()
                        + " with offset "
                        + metadata.offset());
    }

    public static void printConsumerRecord(ConsumerRecord record) {
        System.out.println(
                "Received message: (" + record.key() + ", " + record.value() + ") at " + "topic "
                        + record.topic() + " partition " + record.partition() + " offset "
                        + record.offset());
    }
}