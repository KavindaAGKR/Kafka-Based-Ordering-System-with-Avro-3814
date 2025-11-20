package com.example.kafka.controller;

import com.example.kafka.services.DLQConsumerService;
import com.example.kafka.services.PriceAggregationService;
import com.example.kafka.services.OrderProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService producerService;
    private final PriceAggregationService priceAggregationService;
    private final DLQConsumerService dlqConsumerService;

    public OrderController(OrderProducerService producerService,
            PriceAggregationService priceAggregationService,
            DLQConsumerService dlqConsumerService) {
        this.producerService = producerService;
        this.priceAggregationService = priceAggregationService;
        this.dlqConsumerService = dlqConsumerService;
    }

    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendSingleOrder() {
        producerService.sendOrder();
        Map<String, String> response = new HashMap<>();
        response.put("message", "Order sent successfully");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/send-multiple")
    public ResponseEntity<Map<String, String>> sendMultipleOrders(
            @RequestParam(defaultValue = "10") int count) {
        if (count < 1 || count > 1000) {
            Map<String, String> error = new HashMap<>();
            error.put("error", "Count must be between 1 and 1000");
            return ResponseEntity.badRequest().body(error);
        }

        producerService.sendMultipleOrders(count);
        Map<String, String> response = new HashMap<>();
        response.put("message", count + " orders sent successfully");
        return ResponseEntity.ok(response);
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getOrderStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalOrders", priceAggregationService.getOrderCount());
        stats.put("totalPrice", String.format("%.2f", priceAggregationService.getTotalPrice()));
        stats.put("runningAverage", String.format("%.2f", priceAggregationService.getRunningAverage().get()));
        stats.put("failedOrders", dlqConsumerService.getFailedOrderCount());
        return ResponseEntity.ok(stats);
    }

    
    @GetMapping("/failed")
    public ResponseEntity<Map<String, Object>> getFailedOrders() {
        List<DLQConsumerService.FailedOrder> failedOrders = dlqConsumerService.getFailedOrders();
        Map<String, Object> response = new HashMap<>();
        response.put("count", failedOrders.size());
        response.put("failedOrders", failedOrders);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/stats/reset")
    public ResponseEntity<Map<String, String>> resetStats() {
        priceAggregationService.reset();
        dlqConsumerService.clearFailedOrders();
        Map<String, String> response = new HashMap<>();
        response.put("message", "Statistics reset successfully");
        return ResponseEntity.ok(response);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        Map<String, String> health = new HashMap<>();
        health.put("status", "UP");
        health.put("service", "Kafka Order System");
        return ResponseEntity.ok(health);
    }
}
