package com.example.kafka_producer.jobs;

import com.example.kafka_producer.enums.status;
import com.example.kafka_producer.producers.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestHeader;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

@EnableScheduling
@Slf4j
@Component
public class KafkaProducerJob {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Value("${spring.kafka.topic.status}")
    private String topic;

    @Scheduled(cron = "${cron.status}")
    public void sendStatus() {
        var requestHeaders = getRequestHeaders();

        ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>(topic,
                0,
                System.currentTimeMillis(),
                "kafka-producer",
                getRequestBody(),
                requestHeaders);

        kafkaProducer.sendMessage(producerRecord);
        log.info("Sent message with traceparent {}", extractTraceparent(requestHeaders));
    }

    private String extractTraceparent(RecordHeaders headers) {
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

    private String generateTraceparent() {
        String version = "00"; // W3C Trace Context version 00
        String traceId = UUID.randomUUID().toString().replace("-", ""); // 32 hex chars
        String spanId = UUID.randomUUID().toString().substring(0, 16).replace("-", ""); // 16 hex chars
        String traceFlags = "01"; // Sampled flag set to 1
        return String.format("%s-%s-%s-%s", version, traceId, spanId, traceFlags);
    }

    private String getRequestBody() {
        var statusList = status.values();
        var randomNumber = new Random();
        var randomIndex = randomNumber.nextInt(statusList.length);
        return "{\\\"status\\\":\\\""+statusList[randomIndex]+"\\\"}";
    }

    private RecordHeaders getRequestHeaders() {
        RecordHeaders requestHeaders = new RecordHeaders();
        requestHeaders.add("Content-Type", "application/json".getBytes(StandardCharsets.UTF_8));
        requestHeaders.add("traceparent", generateTraceparent().getBytes(StandardCharsets.UTF_8));
        return requestHeaders;
    }

}
