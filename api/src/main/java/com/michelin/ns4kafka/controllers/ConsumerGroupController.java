package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ConsumerGroup.ConsumerOffset;
import com.michelin.ns4kafka.services.ConsumerGroupService;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


import io.micronaut.http.annotation.*;
import io.micronaut.security.authentication.Authentication;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Tag(name = "Consumer Group management",
        description = "APIs to handle Consumer Group")
@Controller("/api/namespaces/{namespace}/consumer-group")
public class ConsumerGroupController extends NamespacedResourceController {

    @Inject
    ConsumerGroupService consumerGroupService;

    @Post("/{consumerGroupName}/reset")
    void resetOffsets(String namespace, String consumerGroupName, @Body List<ConsumerOffset> offsets) {
        /*
        List.of("topic(:partition)","method", "option");
        List.of("toto","to-earliest", "{}");
        List.of("toto","to-datetime", "{2021-01-01T00:00:00.000}");
        List.of("toto","to-offset",
                Map.of(0, 123,
                        1, 234,
                        2, 345,
                        3, 456)
        );
        */

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


}
