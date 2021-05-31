package com.michelin.ns4kafka.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffset;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffset.ConsumerGroupResetOffsetMethod;
import com.michelin.ns4kafka.services.ConsumerGroupService;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Consumer Group management",
        description = "APIs to handle Consumer Group")
@Controller("/api/namespaces/{namespace}/consumer-group")
public class ConsumerGroupController extends NamespacedResourceController {

    @Inject
    ConsumerGroupService consumerGroupService;

    @Post("/{consumerGroupName}/reset")
    void resetOffsets(String consumerGroupName, @Body ConsumerGroupResetOffset consumerGroupResetOffset) {

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

        try {
            AlterConsumerGroupOffsetsResult result = consumerGroupService.resetOffset(ns, consumerGroupName, consumerGroupResetOffset);
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaConsumerException(e.getMessage());
        }


    }

    @Post("/{consumerGroupName}/to-date-time")
    void toTimeDateOffsets(String consumerGroupName, @Body ConsumerGroupResetOffset consumerGroupResetOffset) {

        Namespace ns = getNamespace(consumerGroupResetOffset.getMetadata().getNamespace());

        // validation
        List<String> validationErrors = new ArrayList<>();
        if (consumerGroupResetOffset.getSpec().getMethod().compareTo(ConsumerGroupResetOffsetMethod.TO_DATE_TIME) != 0) {
            validationErrors.add("Method different of TO_DATE_TIME");
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

        try {
            AlterConsumerGroupOffsetsResult result = consumerGroupService.toTimeDateOffset(ns, consumerGroupName, consumerGroupResetOffset);
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaConsumerException(e.getMessage());
        }


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
