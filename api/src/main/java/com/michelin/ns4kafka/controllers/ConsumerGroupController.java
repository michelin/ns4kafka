package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ConsumerGroup.ConsumerGroupOffset;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.michelin.ns4kafka.services.ConsumerGroupService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.security.authentication.Authentication;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Tag(name = "Consumer Group management",
        description = "APIs to handle Consumer Group")
@Controller("/api/namespaces/{namespace}/consumer-group")
public class ConsumerGroupController extends NamespacedResourceController {

    @Inject
    ConsumerGroupService consumerGroupService;

    @Post("/{name}/reset")
    void resetOffsets(String name ) {
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
        new TopicPartition(name, );

    }

    @Post("/{name}/set-offset")
    void changeOffsets(String name, @Body List<ConsumerGroupOffset> offsets) {
        Map<TopicPartition, OffsetAndMetadata> mapOffset = new HashMap<>();
        offsets.forEach(offset -> {
                mapOffset.put(new TopicPartition(name, offset.getPartition()), new OffsetAndMetadata(offset.getOffset()));
        });
        consumerGroupService.setOffset(name, mapOffset);
    }


}
