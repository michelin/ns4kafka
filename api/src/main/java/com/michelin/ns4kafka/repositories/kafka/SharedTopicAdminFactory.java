package com.michelin.ns4kafka.repositories.kafka;

import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.connect.util.SharedTopicAdmin;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

@Factory
public class SharedTopicAdminFactory {

    @Inject
    KafkaDefaultConfiguration adminConfig;

    @Bean
    @Singleton
    public SharedTopicAdmin build(){
        Map<String, Object> adminProps = new HashMap<>();
        adminConfig.getConfig().forEach((k,v)-> adminProps.put(k.toString(),v));
        return new SharedTopicAdmin(adminProps);
    }
}
