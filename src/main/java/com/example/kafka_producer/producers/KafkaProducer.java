package com.example.kafka_producer.producers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
@Slf4j
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(ProducerRecord<String, String> producerRecord) {
        kafkaTemplate.send(producerRecord);
        log.info("Sent message with traceparent {}", extractTraceparent(producerRecord.headers()));
    }

    private String extractTraceparent(Headers headers) {
        for(var header: headers) {
            if(header.key().equals("traceparent")) {
                var traceparent = new String(header.value(), StandardCharsets.UTF_8);
                log.debug("Traceparent: {}", traceparent);
                return traceparent;
            }
        }

        log.warn("Traceparent not found");
        return null;
    }

}
