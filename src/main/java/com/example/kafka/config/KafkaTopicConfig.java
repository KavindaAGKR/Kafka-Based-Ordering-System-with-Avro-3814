package com.example.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    @Value("${kafka.topic.retry}")
    private String retryTopic;

    @Value("${kafka.topic.dlq}")
    private String dlqTopic;

    @Value("${kafka.topic.aggregated}")
    private String aggregatedTopic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic(ordersTopic, 3, (short) 1);
    }

    @Bean
    public NewTopic retryTopic() {
        return new NewTopic(retryTopic, 3, (short) 1);
    }

    @Bean
    public NewTopic dlqTopic() {
        return new NewTopic(dlqTopic, 3, (short) 1);
    }

    @Bean
    public NewTopic aggregatedTopic() {
        return new NewTopic(aggregatedTopic, 3, (short) 1);
    }
}
