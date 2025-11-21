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

    // Send a specific order with JSON payload
    @PostMapping
    public ResponseEntity<Map<String, String>> sendOrder(@RequestBody Map<String, Object> orderData) {
        try {
            String orderId = (String) orderData.get("orderId");
            String product = (String) orderData.get("product");

            Object priceObj = orderData.get("price");
            float price;
            if (priceObj instanceof Double) {
                price = ((Double) priceObj).floatValue();
            } else if (priceObj instanceof Float) {
                price = (Float) priceObj;
            } else if (priceObj instanceof Integer) {
                price = ((Integer) priceObj).floatValue();
            } else {
                price = Float.parseFloat(priceObj.toString());
            }

            producerService.sendSpecificOrder(orderId, product, price);

            Map<String, String> response = new HashMap<>();
            response.put("message", "Order sent successfully");
            response.put("orderId", orderId);
            response.put("product", product);
            response.put("price", String.valueOf(price));
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("error", "Invalid order data: " + e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    // Send a single random order to Kafka
    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendSingleOrder() {
        producerService.sendOrder();
        Map<String, String> response = new HashMap<>();
        response.put("message", "Order sent successfully");
        return ResponseEntity.ok(response);
    }

    // Send multiple random orders to Kafka
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

    /**
     * Get real-time aggregation statistics
     * - Total orders processed
     * - Total price sum
     * - Running average price
     * - Failed orders count
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getOrderStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalOrders", priceAggregationService.getOrderCount());
        stats.put("totalPrice", String.format("%.2f", priceAggregationService.getTotalPrice()));
        stats.put("runningAverage", String.format("%.2f", priceAggregationService.getRunningAverage().get()));
        stats.put("failedOrders", dlqConsumerService.getFailedOrderCount());

        return ResponseEntity.ok(stats);
    }

    // Get all failed orders from DLQ
    @GetMapping("/failed")
    public ResponseEntity<Map<String, Object>> getFailedOrders() {
        List<DLQConsumerService.FailedOrder> failedOrders = dlqConsumerService.getFailedOrders();
        Map<String, Object> response = new HashMap<>();
        response.put("count", failedOrders.size());
        response.put("failedOrders", failedOrders);
        return ResponseEntity.ok(response);
    }

    // Reset all statistics and clear DLQ
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
