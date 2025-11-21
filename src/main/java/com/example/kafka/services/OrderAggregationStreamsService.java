package com.example.kafka.services;

import com.example.kafka.avro.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafkaStreams
public class OrderAggregationStreamsService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    @Value("${kafka.topic.aggregated}")
    private String aggregatedTopic;

    @Value("${spring.kafka.consumer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-aggregation-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("schema.registry.url", schemaRegistryUrl);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, Order> orderStream = streamsBuilder
                .stream(ordersTopic, Consumed.with(Serdes.String(), getOrderSerde()));

        KGroupedStream<String, Order> groupedByProduct = orderStream
                .filter((key, order) -> order.getPrice() >= 0)
                .peek((key, order) -> log.info("Processing order for aggregation: OrderId={}, Product={}, Price={}",
                        order.getOrderId(), order.getProduct(), order.getPrice()))
                .groupBy((key, order) -> order.getProduct().toString(),
                        Grouped.with(Serdes.String(), getOrderSerde()));

        KTable<String, String> averageTable = groupedByProduct
                .aggregate(
                        () -> "0,0", 
                        (product, order, aggregate) -> {
                            String[] parts = aggregate.split(",");
                            double sum = Double.parseDouble(parts[0]);
                            int count = Integer.parseInt(parts[1]);

                            sum += order.getPrice();
                            count++;

                            return sum + "," + count;
                        },
                        Materialized.with(Serdes.String(), Serdes.String()))
                .mapValues(aggregate -> {
                    String[] parts = aggregate.split(",");
                    double sum = Double.parseDouble(parts[0]);
                    int count = Integer.parseInt(parts[1]);
                    double average = sum / count;
                    return String.format("%.2f", average);
                });

        averageTable.toStream()
                .peek((product, average) -> log.info("Aggregated Avg - {} = {}", product, average))
                .to(aggregatedTopic, Produced.with(Serdes.String(), Serdes.String()));

        return averageTable.toStream();
    }

    private org.apache.kafka.common.serialization.Serde<Order> getOrderSerde() {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("schema.registry.url", schemaRegistryUrl);
        serdeProps.put("specific.avro.reader", true);

        io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde<Order> orderSerde = new io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde<>();
        orderSerde.configure(serdeProps, false);

        return orderSerde;
    }
}