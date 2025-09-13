package com.example.kafka_producer.jobs;

import com.example.kafka_producer.enums.Status;
import com.example.kafka_producer.models.OrderModel;
import com.example.kafka_producer.producers.KafkaProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
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
    }

    private String generateTraceparent() {
        String version = "00"; // W3C Trace Context version 00
        String traceId = UUID.randomUUID().toString().replace("-", ""); // 32 hex chars
        String spanId = UUID.randomUUID().toString().substring(0, 16).replace("-", ""); // 16 hex chars
        String traceFlags = "01"; // Sampled flag set to 1
        return String.format("%s-%s-%s-%s", version, traceId, spanId, traceFlags);
    }

    private String getRequestBody() {
        var statusList = Status.values();
        var randomNumber = new Random();
        var randomIndex = randomNumber.nextInt(statusList.length);

        var orderModel = new OrderModel();
        orderModel.setOrderNumber(randomNumber.nextInt(8));
        orderModel.setOrderStatus(statusList[randomIndex].toString());

        var objectMapper = new ObjectMapper();
        String orderModelMapper = null;
        try {
            orderModelMapper = objectMapper.writeValueAsString(orderModel);
        } catch (JsonProcessingException jpe) {
            log.error("Error serializing order model: {}", jpe.getMessage(), jpe);
            throw new RuntimeException(jpe);
        }

        return orderModelMapper;
    }

    private RecordHeaders getRequestHeaders() {
        RecordHeaders requestHeaders = new RecordHeaders();
        requestHeaders.add("Content-Type", "application/json".getBytes(StandardCharsets.UTF_8));
        requestHeaders.add("traceparent", generateTraceparent().getBytes(StandardCharsets.UTF_8));
        return requestHeaders;
    }

}
