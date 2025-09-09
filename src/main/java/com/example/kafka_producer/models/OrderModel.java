package com.example.kafka_producer.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderModel {
    int orderNumber;
    String orderStatus;
}
