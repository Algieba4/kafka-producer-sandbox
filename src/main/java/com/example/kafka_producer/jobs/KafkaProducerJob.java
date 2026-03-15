package com.example.kafka_producer.jobs;

import com.example.kafka_producer.enums.Status;
import com.example.kafka_producer.models.OrderModel;
import com.example.kafka_producer.producers.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.Random;

@EnableScheduling
@Slf4j
@Component
public class KafkaProducerJob {

    private final KafkaProducer kafkaProducer;

    @Value("${spring.kafka.topic.status}")
    private String topic;

    public KafkaProducerJob(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

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

    private String getRequestBody() {
        var statusList = Status.values();
        var randomNumber = new Random();
        var randomIndex = randomNumber.nextInt(statusList.length);

        var orderModel = new OrderModel();
        orderModel.setOrderNumber(randomNumber.nextInt(90000000) + 10000000);
        orderModel.setOrderStatus(statusList[randomIndex].toString());

        var jsonMapper = new JsonMapper();
        return jsonMapper.writeValueAsString(orderModel);
    }

    private RecordHeaders getRequestHeaders() {
        RecordHeaders requestHeaders = new RecordHeaders();
        requestHeaders.add("Content-Type", "application/json".getBytes(StandardCharsets.UTF_8));
        return requestHeaders;
    }

}
