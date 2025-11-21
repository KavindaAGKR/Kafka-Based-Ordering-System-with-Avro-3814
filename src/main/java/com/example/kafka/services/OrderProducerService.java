package com.example.kafka.services;

import com.example.kafka.avro.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class OrderProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String ordersTopic;
    private final Random random = new Random();
    private final String[] products = { "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Webcam", "Tablet",
            "Smartphone", "Charger", "USB Cable" };

    public OrderProducerService(KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${kafka.topic.orders}") String ordersTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordersTopic = ordersTopic;
    }

    public void sendOrder() {
        Order order = createRandomOrder();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(ordersTopic,
                order.getOrderId().toString(), order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent successfully: OrderId={}, Product={}, Price=${}",
                        order.getOrderId(), order.getProduct(), order.getPrice());
            } else {
                log.error("Failed to send order: OrderId={}, Error={}",
                        order.getOrderId(), ex.getMessage());
            }
        });
    }

    public void sendMultipleOrders(int count) {
        for (int i = 0; i < count; i++) {
            sendOrder();
        }
        log.info("Sent {} orders to Kafka topic: {}", count, ordersTopic);
    }

    public void sendSpecificOrder(String orderId, String product, float price) {
        Order order = Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(product)
                .setPrice(price)
                .build();

        kafkaTemplate.send(ordersTopic, order.getOrderId().toString(), order);
        log.info("Sent specific order: OrderId={}, Product={}, Price=${}",
                order.getOrderId(), order.getProduct(), order.getPrice());
    }

    private Order createRandomOrder() {
        String orderId = UUID.randomUUID().toString();
        String product = products[random.nextInt(products.length)];
        float price = 10.0f + (random.nextFloat() * 990.0f); 

        return Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(product)
                .setPrice(price)
                .build();
    }
}
