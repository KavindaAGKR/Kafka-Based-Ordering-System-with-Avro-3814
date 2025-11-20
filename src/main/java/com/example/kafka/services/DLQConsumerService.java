package com.example.kafka.services;

import com.example.kafka.avro.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
public class DLQConsumerService {

    private final List<FailedOrder> failedOrders = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "${kafka.topic.dlq}", groupId = "dlq-consumer-group")
    public void consumeDLQOrder(@Payload Order order,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {

        log.error("DLQ ORDER RECEIVED");

        log.error("OrderId: {}", order.getOrderId());
        log.error("Product: {}", order.getProduct());
        log.error("Price: ${}", order.getPrice());
        log.error("Timestamp: {}", LocalDateTime.now());

        FailedOrder failedOrder = new FailedOrder(
                order.getOrderId().toString(),
                order.getProduct().toString(),
                order.getPrice(),
                LocalDateTime.now(),
                "Failed after max retry attempts");
        failedOrders.add(failedOrder);

        acknowledgment.acknowledge();

        log.warn("Total failed orders in DLQ: {}", failedOrders.size());
    }

    public List<FailedOrder> getFailedOrders() {
        return new ArrayList<>(failedOrders);
    }

    public int getFailedOrderCount() {
        return failedOrders.size();
    }

    public void clearFailedOrders() {
        failedOrders.clear();
        log.info("Failed orders list cleared");
    }

    public record FailedOrder(
            String orderId,
            String product,
            float price,
            LocalDateTime failedAt,
            String reason) {
    }
}
