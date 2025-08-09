package com.example.kafka_producer_sandbox.controllers;

import com.example.kafka_producer_sandbox.producers.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@RestController
@RequestMapping(path = "/api/v1/publish")
@Slf4j
public class KafkaProducerController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Value("${spring.kafka.topic}")
    private String topic;

    @PutMapping(value="/")
    public void publishMessage(@RequestHeader Map<String, String> headers,
                               @RequestBody String requestBody) {

        RecordHeaders requestHeaders = new RecordHeaders();
        headers.forEach((key, value) ->
            requestHeaders.add(key, headers.get(key).getBytes(StandardCharsets.UTF_8)));

        ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>(topic,
                0,
                System.currentTimeMillis(),
                "kafka-producer-sandbox",
                requestBody,
                requestHeaders);

        try {
            kafkaProducer.sendMessage(producerRecord);
        } catch (Exception e) {
            log.error("Unhandled exception while trying to producer message: {}",
                e.getMessage(),
                e);
        }

    }

}
