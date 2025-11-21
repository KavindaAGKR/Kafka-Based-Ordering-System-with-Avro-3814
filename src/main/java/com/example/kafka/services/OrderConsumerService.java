package com.example.kafka.services;

import com.example.kafka.avro.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Random;

@Slf4j
@Service
public class OrderConsumerService {

    private final PriceAggregationService priceAggregationService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String retryTopic;
    private final Random random = new Random();

    public OrderConsumerService(PriceAggregationService priceAggregationService,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${kafka.topic.retry}") String retryTopic) {
        this.priceAggregationService = priceAggregationService;
        this.kafkaTemplate = kafkaTemplate;
        this.retryTopic = retryTopic;
    }

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeOrder(@Payload Order order,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        try {
            log.info("Consumed order: OrderId={}, Product={}, Price=${}",
                    order.getOrderId(), order.getProduct(), order.getPrice());

            if (order.getPrice() < 0) {
                throw new IllegalArgumentException(
                        "Invalid price: " + order.getPrice() + " - Price cannot be negative");
            }

            if (random.nextInt(10) == 0) {
                throw new RuntimeException("Simulated temporary processing failure");
            }

            processOrder(order);
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing order: OrderId={}, Error={} - Sending to retry topic",
                    order.getOrderId(), e.getMessage());
            sendToRetry(order);
            acknowledgment.acknowledge();
        }
    }

    private void processOrder(Order order) {
        priceAggregationService.addOrderPrice(order.getPrice());
        log.info("Order processed successfully: OrderId={}", order.getOrderId());
    }

    private void sendToRetry(Order order) {
        log.warn("Sending order to retry topic: OrderId={}", order.getOrderId());
        kafkaTemplate.send(retryTopic, order.getOrderId().toString(), order);
    }
}
