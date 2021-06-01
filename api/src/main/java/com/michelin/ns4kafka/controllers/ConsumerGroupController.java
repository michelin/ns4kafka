package com.michelin.ns4kafka.controllers;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.validation.Valid;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetStatus;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.ConsumerGroupService;

import org.apache.kafka.common.TopicPartition;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.tags.Tag;

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

        Map<String, Long> finalOffsets = null;

        try {
            List<TopicPartition> partitionsToReset = consumerGroupService.getPartitionsToReset(ns, consumerGroup, consumerGroupResetOffsets.getSpec().getTopic());
            Map<TopicPartition,Long> preparedOffsets = consumerGroupService.prepareOffsetsToReset(ns,consumerGroup, consumerGroupResetOffsets.getSpec().getOptions(), partitionsToReset, consumerGroupResetOffsets.getSpec().getMethod());
            if (!dryrun) {
                finalOffsets = consumerGroupService.apply(ns,consumerGroup ,preparedOffsets);
            }

        } catch (Exception e) {
            ConsumerGroupResetOffsetStatus status =  ConsumerGroupResetOffsetStatus.builder()
                .success(false)
                .errorMessage(e.getMessage())
                .build();

            consumerGroupResetOffsets.setStatus(status);
            return consumerGroupResetOffsets;
        }

        ConsumerGroupResetOffsetStatus status =  ConsumerGroupResetOffsetStatus.builder()
            .success(true)
            .offsetChanged(finalOffsets)
            .build();

        consumerGroupResetOffsets.setStatus(status);
        return consumerGroupResetOffsets;


    }


}
