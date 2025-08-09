package com.example.kafka_producer_sandbox.controllers;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@AutoConfigureMockMvc
@DirtiesContext
@EmbeddedKafka(partitions=1)
@Slf4j
@SpringBootTest
public class KafkaProducerControllerTests {

    @Autowired
    private KafkaProducerController kafkaProducerController;

    private MockMvc mockMvc;

    private JSONArray requestBody;

    @BeforeEach
    public void setup() throws JSONException {
        mockMvc = MockMvcBuilders.standaloneSetup(kafkaProducerController).build();
        requestBody = createRequestBody();
    }

    @Test
    public void testEndpointWithTraceparent() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/v1/publish/")
            .contentType(MediaType.APPLICATION_JSON)
            .content(String.valueOf(requestBody))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
            .header("traceparent", "00-0123456789101112-12345678-01")
        )
        .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testEndpointWithoutTraceparent() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/v1/publish/")
            .contentType(MediaType.APPLICATION_JSON)
            .content(String.valueOf(requestBody))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        )
        .andExpect(MockMvcResultMatchers.status().isOk());
    }

    private JSONArray  createRequestBody() throws JSONException {
        JSONArray requestBody = new JSONArray();
        JSONObject requestBodyObject = new JSONObject();
        requestBodyObject.put("message", "This is a test message");
        requestBody.put(requestBodyObject);
        return requestBody;
    }
}
