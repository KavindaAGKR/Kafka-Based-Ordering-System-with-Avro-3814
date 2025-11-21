# Kafka-Based Ordering System with Avro Serialization

**EC8202 - Big Data and Analytics Assignment - 01**

A Spring Boot application demonstrating Kafka message streaming with Avro serialization, retry logic, and Dead Letter Queue pattern for order processing.

## Overview

This project implements a distributed order processing system using Apache Kafka as the message broker. Orders are serialized using Apache Avro schema and processed through multiple Kafka topics with built-in failure handling and retry mechanisms.

## System Architecture

The system consists of three main components:

1. **Order Processing** - Consumes orders from the main topic and processes them
2. **Retry On Failures** - Handles temporary failures
3. **Dead Letter Queue** - Manages permanently failed messages after retry attempts
4. **Real-time Aggregation** - Calculates running statistics of processed orders

## How It Works

1. Client sends order via REST API
2. Producer publishes order to Kafka topic using Avro serialization
3. Consumer processes the order with validation and error handling
4. Successfully processed orders update real-time price aggregation
5. Failed orders are sent to retry topic with exponential backoff
6. Orders failing all retry attempts go to Dead Letter Queue
7. DLQ consumer logs failed messages for investigation and monitoring

## Prerequisites

- Java 21
- Maven 3.6+
- Docker

## Quick Start

1. Start Kafka infrastructure:
```bash
docker-compose up -d
```

2. Build and run the application:
```bash
mvn clean compile
mvn spring-boot:run
```

3. Send test orders:
```bash
curl -X POST http://localhost:8080/api/orders/send-multiple?count=20
```

4. View statistics:
```bash
curl http://localhost:8080/api/orders/stats
```

## Message Flow

Orders flow through the following topics:
- `orders-topic` - Main order processing
- `orders-retry-topic` - Failed orders for retry
- `orders-dlq-topic` - Permanently failed orders

## Message Flow Architecture

```
┌─────────────────┐
│  Order Producer │
└────────┬────────┘
         │
         ▼
  [orders-topic]
         │
         ▼
┌─────────────────────┐
│  Order Consumer     │ ──► [Price Aggregation]
└────────┬────────────┘
         │
         │ (order fails)
         ▼
  [orders-retry-topic]
         │
         ▼
┌─────────────────────┐
│  Retry Consumer     │
│  (Max 3 attempts)   │
└────────┬────────────┘
         │
         │ (Max retries)
         ▼
  [orders-dlq-topic]
         │
         ▼
┌─────────────────────┐
│  DLQ Consumer       │ ──► [Log & Store]
└─────────────────────┘
```


## API Endpoints

- `POST /api/orders/send` - Send single order
- `POST /api/orders/send-multiple?count=N` - Send multiple orders
- `GET /api/orders/stats` - View processing statistics
- `GET /api/orders/failed` - View failed orders


