package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetStatus;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.ConsumerGroupService;
import com.michelin.ns4kafka.services.ConsumerGroupService.resultWithOffsets;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Tag(name = "Consumer Groups")
@Controller("/api/namespaces/{namespace}/consumer-groups")
public class ConsumerGroupController extends NamespacedResourceController {

    @Inject
    ConsumerGroupService consumerGroupService;

    @Post("/{consumerGroup}/reset{?dryrun}")
    public ConsumerGroupResetOffsets resetOffsets(String namespace, String consumerGroup,
                                                  @Valid @Body ConsumerGroupResetOffsets consumerGroupResetOffsets,
                                                  @QueryValue(defaultValue = "false") boolean dryrun) {

        Namespace ns = getNamespace(namespace);

        // validate spec
        List<String> validationErrors = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        // validate ownership
        if (!consumerGroupService.isNamespaceOwnerOfConsumerGroup(namespace, consumerGroup)) {
            validationErrors.add("Invalid value " + consumerGroup + " for name: Namespace not OWNER of this consumer group");
        }
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        // TODO THIS
        //  val partitionsToReset = getPartitionsToReset(groupId)
        //  val preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset)
        //  if dry-run return preparedOffsets
        //
        resultWithOffsets result;
        try {
            result = consumerGroupService.resetOffset(ns, consumerGroup, consumerGroupResetOffsets);

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

        consumerGroupResetOffsets.setStatus(status);
        return consumerGroupResetOffsets;


    }

    @Post("/{consumerGroupName}/to-datetime{?dryrun}")
    ConsumerGroupResetOffsets toTimeDateOffsets(String consumerGroupName, @Body ConsumerGroupResetOffsets consumerGroupResetOffsets, @QueryValue(defaultValue = "false") boolean dryrun) {

        Namespace ns = getNamespace(consumerGroupResetOffsets.getMetadata().getNamespace());

        // validation
        List<String> validationErrors = new ArrayList<>();
        if (consumerGroupResetOffsets.getSpec().getMethod().compareTo(ResetOffsetsMethod.TO_DATETIME) != 0) {
            validationErrors.add("Method different of TO_DATETIME");
        }
        if (!consumerGroupService.isNamespaceOwnerOfConsumerGroup(consumerGroupResetOffsets.getMetadata().getNamespace(), consumerGroupName)) {
            validationErrors.add("Namespace is not owner of group");
        }
        if (consumerGroupResetOffsets.getSpec().getTimestamp() == null) {
            validationErrors.add("Timestamp is null");
        }
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        resultWithOffsets result;
        try {
             result = consumerGroupService.toTimeDateOffset(ns, consumerGroupName, consumerGroupResetOffsets);

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

        consumerGroupResetOffsets.setStatus(status);
        return consumerGroupResetOffsets;


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
