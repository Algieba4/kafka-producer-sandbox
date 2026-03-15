package com.example.kafka_producer.producers;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@EmbeddedKafka(partitions = 1)
class KafkaProducerTests {

    private final String topic = "test-topic";

    private KafkaProducer kafkaProducer;
    private KafkaTemplate<String, String> kafkaTemplate;
    private JSONObject requestBody;

    @BeforeEach
    void setup() throws JSONException {
        kafkaTemplate = mock(KafkaTemplate.class);
        kafkaProducer = new  KafkaProducer(kafkaTemplate);
        requestBody = createRequestBody();
    }

    @Test
    void test_publish_message_into_kafka() {
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

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, times(1)).send(captor.capture());

        ProducerRecord<String, String> producerRecordCaptor = captor.getValue();
        assertNotNull(producerRecordCaptor.key());
        assertNotNull(producerRecordCaptor.value());
        assertEquals("test-topic", producerRecordCaptor.topic());
    }

    private JSONObject createRequestBody() throws JSONException {
        JSONObject requestBodyObject = new JSONObject();
        requestBodyObject.put("orderNumber", "000000");
        return requestBodyObject;
    }

}
