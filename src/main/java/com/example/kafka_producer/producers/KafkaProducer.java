package com.example.kafka_producer.producers;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
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
        log.info("Sent order {} with traceparent {}",
            getOrderNumber(producerRecord.value()),
            getTraceparent(producerRecord.headers()));
    }

    private String getOrderNumber(String requestBody) {
        String orderNumber = null;
        if (requestBody != null) {
            Gson gson = new Gson();
            var jsonObject = gson.fromJson(requestBody, JsonObject.class);
            orderNumber =  jsonObject.get("orderNumber").toString();
        }
        return orderNumber;
    }

    private String getTraceparent(Headers headers) {
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
