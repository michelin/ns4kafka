package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.TagSpecs;
import com.michelin.ns4kafka.services.clients.schema.entities.TagTopicInfo;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.michelin.ns4kafka.utils.config.ClusterConfig.CLUSTER_ID;
import static com.michelin.ns4kafka.utils.tags.TagsUtils.TOPIC_ENTITY_TYPE;

public class TopicAsyncExecutorMock extends TopicAsyncExecutor {

    public TopicAsyncExecutorMock(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        super(kafkaAsyncExecutorConfig);
    }

    public void setSchemaRegistryClient(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

}
