package com.michelin.ns4kafka.controllers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffset;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffset.ConsumerGroupResetOffsetMethod;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffset.ConsumerGroupResetOffsetStatus;
import com.michelin.ns4kafka.services.ConsumerGroupService;
import com.michelin.ns4kafka.services.ConsumerGroupService.resultWithOffsets;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.common.TopicPartition;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Consumer Group management",
        description = "APIs to handle Consumer Group")
@Controller("/api/namespaces/{namespace}/consumer-group")
public class ConsumerGroupController extends NamespacedResourceController {

    @Inject
    ConsumerGroupService consumerGroupService;

    @Post("/{consumerGroupName}/reset{?dryrun}")
    ConsumerGroupResetOffset resetOffsets(String consumerGroupName, @Body ConsumerGroupResetOffset consumerGroupResetOffset, @QueryValue(defaultValue = "false") boolean dryrun) {

        Namespace ns = getNamespace(consumerGroupResetOffset.getMetadata().getNamespace());

        // validation
        List<String> validationErrors = new ArrayList<>();
        if (consumerGroupResetOffset.getSpec().getMethod().compareTo(ConsumerGroupResetOffsetMethod.TO_EARLIEST) != 0) {
            validationErrors.add("Method different of TO_EARLIEST");
        }
        if (!consumerGroupService.isNamespaceOwnerOfConsumerGroup(consumerGroupResetOffset.getMetadata().getNamespace(), consumerGroupName)) {
            validationErrors.add("Namespace is not owner of group");
        }
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        resultWithOffsets result;
        try {
            result = consumerGroupService.resetOffset(ns, consumerGroupName, consumerGroupResetOffset);

            if (!dryrun) {
                result.getResult().all().get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaConsumerException(e.getMessage());
        }

        Map<String, Long> mapResult = new HashMap<>();
        result.getMapOffset().forEach((topicPartition, offsetAndMetadata) -> {
                mapResult.put(topicPartition.topic() + topicPartition.partition(), offsetAndMetadata.offset());
        });

        ConsumerGroupResetOffsetStatus status =  ConsumerGroupResetOffsetStatus.builder()
            .success(true)
            .offsetChanged(mapResult)
            .build();

        consumerGroupResetOffset.setStatus(status);
        return consumerGroupResetOffset;


    }

    @Post("/{consumerGroupName}/to-datetime{?dryrun}")
    ConsumerGroupResetOffset toTimeDateOffsets(String consumerGroupName, @Body ConsumerGroupResetOffset consumerGroupResetOffset, @QueryValue(defaultValue = "false") boolean dryrun) {

        Namespace ns = getNamespace(consumerGroupResetOffset.getMetadata().getNamespace());

        // validation
        List<String> validationErrors = new ArrayList<>();
        if (consumerGroupResetOffset.getSpec().getMethod().compareTo(ConsumerGroupResetOffsetMethod.TO_DATETIME) != 0) {
            validationErrors.add("Method different of TO_DATETIME");
        }
        if (!consumerGroupService.isNamespaceOwnerOfConsumerGroup(consumerGroupResetOffset.getMetadata().getNamespace(), consumerGroupName)) {
            validationErrors.add("Namespace is not owner of group");
        }
        if (consumerGroupResetOffset.getSpec().getTimestamp() == null) {
            validationErrors.add("Timestamp is null");
        }
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        resultWithOffsets result;
        try {
             result = consumerGroupService.toTimeDateOffset(ns, consumerGroupName, consumerGroupResetOffset);

            if (!dryrun) {
                result.getResult().all().get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaConsumerException(e.getMessage());
        }
        Map<String, Long> mapResult = new HashMap<>();
        result.getMapOffset().forEach((topicPartition, offsetAndMetadata) -> {
                mapResult.put(topicPartition.topic() + topicPartition.partition(), offsetAndMetadata.offset());
        });

        ConsumerGroupResetOffsetStatus status =  ConsumerGroupResetOffsetStatus.builder()
            .success(true)
            .offsetChanged(mapResult)
            .build();

        consumerGroupResetOffset.setStatus(status);
        return consumerGroupResetOffset;


    }
    /*
    @Post("/{consumerGroupName}/set-offset")
    void changeOffsets(String namespace, String consumerGroupName, @Body List<ConsumerOffset> offsets) {

        Namespace ns = getNamespace(namespace);
        String cluster = ns.getMetadata().getCluster();

        Map<TopicPartition, OffsetAndMetadata> mapOffset = new HashMap<>();
        offsets.forEach(consumerOffset -> {
                consumerOffset.getPartitionOffsets().forEach( partitionOffset -> {
                        mapOffset.put(new TopicPartition(consumerOffset.getTopic(), partitionOffset.getPartition()), new OffsetAndMetadata(partitionOffset.getOffset()));
                });
        });
        AlterConsumerGroupOffsetsResult result = consumerGroupService.setOffset(cluster,consumerGroupName, mapOffset);
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaConsumerException(e.getMessage());
        }
    }
    */

}
