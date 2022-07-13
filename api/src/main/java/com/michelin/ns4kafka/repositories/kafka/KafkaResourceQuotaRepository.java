package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaResourceQuotaRepository extends KafkaStore<ResourceQuota> implements ResourceQuotaRepository {
    /**
     * Constructor
     * @param kafkaTopic The resource quota topic
     * @param kafkaProducer The resource quota producer
     */
    public KafkaResourceQuotaRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.resource-quotas") String kafkaTopic,
                                      @KafkaClient("role-resource-quotas") Producer<String, ResourceQuota> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(ResourceQuota message) {
        return message.getMetadata().getNamespace();
    }

    /**
     * Get resource quota by namespace
     * @param namespace The namespace used to research
     * @return A resource quota
     */
    @Override
    public Optional<ResourceQuota> findForNamespace(String namespace) {
        return getKafkaStore().values()
                .stream()
                .filter(resourceQuota -> resourceQuota.getMetadata().getNamespace().equals(namespace))
                .findFirst();
    }

    /**
     * Consume messages from resource quotas topic
     * @param record The resource quota message
     */
    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.resource-quotas")
    void receive(ConsumerRecord<String, ResourceQuota> record) {
        super.receive(record);
    }

    /**
     * Produce a resource quota message
     * @param resourceQuota The resource quota to create
     * @return The created resource quota
     */
    @Override
    public ResourceQuota create(ResourceQuota resourceQuota) {
        return produce(getMessageKey(resourceQuota),resourceQuota);
    }

    /**
     * Delete a resource quota message by pushing a tomb stone message
     * @param resourceQuota The resource quota to delete
     */
    @Override
    public void delete(ResourceQuota resourceQuota) {
        produce(getMessageKey(resourceQuota),null);
    }
}
