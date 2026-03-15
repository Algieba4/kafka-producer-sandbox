package com.example.kafka_producer.controllers;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.client.RestTestClient;

@ActiveProfiles("test")
@EmbeddedKafka(partitions=1)
@WebMvcTest(KafkaProducerController.class)
class KafkaProducerControllerTests {

    @MockitoBean
    KafkaProducerController kafkaProducerController;

    @Autowired
    MockMvc mockMvc;

    JSONArray requestBody;
    RestTestClient client;

    @BeforeEach
    void setup() throws JSONException {
        client = RestTestClient.bindTo(mockMvc).build();
        requestBody = createRequestBody();
    }

    @Test
    void test_publish_put_request() {
        client.put()
            .uri("/api/v1/publish/")
            .contentType(MediaType.APPLICATION_JSON)
            .headers(this::getHeaders)
            .header("traceparent", "00-0123456789101112-12345678-01")
            .body(String.valueOf(requestBody))
            .exchange()
            .expectStatus().isOk();
    }

    private JSONArray  createRequestBody() throws JSONException {
        JSONArray request = new JSONArray();
        JSONObject requestBodyObject = new JSONObject();
        requestBodyObject.put("message", "This is a test message");
        request.put(requestBodyObject);
        return request;
    }

    private void getHeaders(HttpHeaders headers) {
        headers.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON.toString());
    }

}
