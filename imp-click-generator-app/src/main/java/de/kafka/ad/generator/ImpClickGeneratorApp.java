package de.kafka.ad.generator;

import java.util.Properties;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import de.kafka.protocol.Utils;
import de.kafka.protocol.constant.Topics;
import de.kafka.protocol.event.ClickEvent;
import de.kafka.protocol.event.ImpressionEvent;
import de.kafka.protocol.generator.RandomGenerator;
import de.kafka.protocol.serde.CustomJsonSerializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImpClickGeneratorApp {

    public static void main(String[] args) {

        if (args.length != 1 ) {
            log.error("pass --bootstrap-servers of Kafka Cluster as first argument");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        if (StringUtils.isBlank(bootstrapServers)) {
            log.error("pass --bootstrap-servers of Kafka Cluster as first argument");
            System.exit(1);
        }

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomJsonSerializer.class.getName());

        try (Producer<String, Object> producer = new KafkaProducer<>(props)) {
            while (true) {
                long currentTimestamp = System.currentTimeMillis();

                ImpressionEvent impressionEvent = RandomGenerator.generateImpressionEvent(currentTimestamp);

                ProducerRecord<String, Object> impData = new ProducerRecord<>(Topics.TOPIC_IMP,
                        impressionEvent.getImpId(),
                        impressionEvent);
                producer.send(impData, (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Fail to produce record " + impressionEvent);
                            } else {
                                Utils.printRecordMetadata(metadata);
                            }
                        }
                );

                Thread.sleep(500);

                if(RandomUtils.nextBoolean()) {
                    long impClickGap = RandomUtils.nextLong(100, 60 * 1000);
                    ClickEvent clickEvent = RandomGenerator.generateClickEvent(currentTimestamp + impClickGap,
                            impressionEvent.getImpId());
                    ProducerRecord<String, Object> clickData = new ProducerRecord<>(Topics.TOPIC_CLICK,
                            clickEvent.getImpId(),
                            clickEvent);
                    producer.send(clickData, (metadata, exception) -> {
                                if (exception != null) {
                                    log.error("Fail to produce record " + clickEvent);
                                } else {
                                    Utils.printRecordMetadata(metadata);
                                }
                            }
                    );
                }
                Thread.sleep(500);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }
}