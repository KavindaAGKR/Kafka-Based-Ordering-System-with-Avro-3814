package com.example.kafka.services;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class PriceAggregationService {

    private final AtomicInteger orderCount = new AtomicInteger(0);
    private final AtomicReference<Double> totalPrice = new AtomicReference<>(0.0);

    @Getter
    private final AtomicReference<Double> runningAverage = new AtomicReference<>(0.0);


    public void addOrderPrice(float price) {
        int count = orderCount.incrementAndGet();
        double total = totalPrice.updateAndGet(current -> current + price);
        double average = total / count;
        runningAverage.set(average);

        log.info("Price Aggregation: Count={}, Total=${:.2f}, Running Average=${:.2f}",
                count, total, average);
    }

    public int getOrderCount() {
        return orderCount.get();
    }

    public double getTotalPrice() {
        return totalPrice.get();
    }

    public void reset() {
        orderCount.set(0);
        totalPrice.set(0.0);
        runningAverage.set(0.0);
        log.info("Aggregation metrics reset");
    }
}
