package com.harvicom.kafkastreams.datetimefix.config;

import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

    @Bean
    public StreamsConfig streamsConfig(KafkaProperties properties){
        //Map<String, Object> props = new HashMap<>();
        //props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return new StreamsConfig(properties.buildStreamsProperties());
    }
}
