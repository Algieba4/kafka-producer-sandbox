package com.example.kafka_producer_sandbox.producers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.nio.charset.StandardCharsets;

@DirtiesContext
@EmbeddedKafka(partitions = 1)
@Slf4j
@SpringBootTest
public class KafkaProducerTests {

    @Autowired
    private KafkaProducer kafkaProducer;

    private JSONArray requestBody;

    @Value("${spring.kafka.topic}")
    private String topic;

    @BeforeEach
    public void setup() throws JSONException {
        requestBody = createRequestBody();
    }

    @Test
    public void testKafkaProducer() throws JSONException {
        RecordHeaders headers = new RecordHeaders();
        headers.add("traceparent", "00-0123456789101213-12345678-01".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>(topic,
                0,
                System.currentTimeMillis(),
                "kafka-producer-sandbox-test",
                requestBody.toString(),
                headers);
        kafkaProducer.sendMessage(producerRecord);

    }

    private JSONArray  createRequestBody() throws JSONException {
        JSONArray requestBody = new JSONArray();
        JSONObject requestBodyObject = new JSONObject();
        requestBodyObject.put("message", "This is a test message");
        requestBody.put(requestBodyObject);
        return requestBody;
    }

}
