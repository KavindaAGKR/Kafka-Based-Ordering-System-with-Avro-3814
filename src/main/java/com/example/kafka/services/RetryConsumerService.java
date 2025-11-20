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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class RetryConsumerService {

    private final PriceAggregationService priceAggregationService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String retryTopic;
    private final String dlqTopic;
    private final int maxRetryAttempts;
    private final long backoffMs;

    private final Map<String, Integer> retryAttempts = new ConcurrentHashMap<>();

    public RetryConsumerService(PriceAggregationService priceAggregationService,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${kafka.topic.retry}") String retryTopic,
            @Value("${kafka.topic.dlq}") String dlqTopic,
            @Value("${kafka.retry.max-attempts}") int maxRetryAttempts,
            @Value("${kafka.retry.backoff-ms}") long backoffMs) {
        this.priceAggregationService = priceAggregationService;
        this.kafkaTemplate = kafkaTemplate;
        this.retryTopic = retryTopic;
        this.dlqTopic = dlqTopic;
        this.maxRetryAttempts = maxRetryAttempts;
        this.backoffMs = backoffMs;
    }

    @KafkaListener(topics = "${kafka.topic.retry}", groupId = "retry-consumer-group")
    public void consumeRetryOrder(@Payload Order order,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        String orderId = order.getOrderId().toString();
        int currentAttempt = retryAttempts.getOrDefault(orderId, 0) + 1;

        log.info("Retry attempt {} for order: OrderId={}", currentAttempt, orderId);

        try {
            long waitTime = backoffMs * currentAttempt;
            Thread.sleep(waitTime);
            log.info("Waited {}ms before retry attempt", waitTime);

            if (Math.random() < 0.5) {
                throw new RuntimeException("Simulated retry failure");
            }

            processOrder(order);

            retryAttempts.remove(orderId);
            acknowledgment.acknowledge();

            log.info("Order processed successfully on retry: OrderId={}, Attempt={}",
                    orderId, currentAttempt);

        } catch (Exception e) {
                log.error("Retry failed for order: OrderId={}, Attempt={}/{}, Error={}",
                    orderId, currentAttempt, maxRetryAttempts, e.getMessage());

            if (currentAttempt >= maxRetryAttempts) {
                sendToDLQ(order, "Max retry attempts reached: " + maxRetryAttempts);
                retryAttempts.remove(orderId);
                acknowledgment.acknowledge();
            } else {
                
                retryAttempts.put(orderId, currentAttempt);
                kafkaTemplate.send(retryTopic, orderId, order);
                acknowledgment.acknowledge();
            }
        }
    }

    private void processOrder(Order order) {
        priceAggregationService.addOrderPrice(order.getPrice());
        log.info("Order processed in retry: OrderId={}", order.getOrderId());
    }

    private void sendToDLQ(Order order, String reason) {
        log.error("Sending order to DLQ: OrderId={}, Reason={}", order.getOrderId(), reason);
        kafkaTemplate.send(dlqTopic, order.getOrderId().toString(), order);
    }
}
