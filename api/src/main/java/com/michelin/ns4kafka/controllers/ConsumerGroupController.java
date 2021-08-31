package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetStatus;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.ConsumerGroupService;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            throw new ResourceValidationException(validationErrors, "ConsumerGroup", consumerGroup);
        }

        ConsumerGroupResetOffsetStatus status = null;
        try {
            // Starting from here, all the code is from Kafka kafka-consumer-group command
            // https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/admin/ConsumerGroupCommand.scala
            // validate Consumer Group is Dead or Inative
            String currentState = consumerGroupService.getConsumerGroupStatus(ns, consumerGroup);
            if (!List.of("Empty", "Dead").contains(currentState)) {
                throw new IllegalStateException("Assignments can only be reset if the group '" + consumerGroup + "' is inactive, but the current state is " + currentState + ".");
            }
            // list partitions
            List<TopicPartition> partitionsToReset = consumerGroupService.getPartitionsToReset(ns, consumerGroup, consumerGroupResetOffsets.getSpec().getTopic());
            // prepare offsets
            Map<TopicPartition, Long> preparedOffsets = consumerGroupService.prepareOffsetsToReset(ns, consumerGroup, consumerGroupResetOffsets.getSpec().getOptions(), partitionsToReset, consumerGroupResetOffsets.getSpec().getMethod());
            if (!dryrun) {
                sendEventLog("ConsumerGroupResetOffsets",
                        consumerGroupResetOffsets.getMetadata(),
                        ApplyStatus.changed,
                        null,
                        consumerGroupResetOffsets.getSpec());
                consumerGroupService.alterConsumerGroupOffsets(ns, consumerGroup, preparedOffsets);
            }
            status = ConsumerGroupResetOffsetStatus.ofSuccess(
                    preparedOffsets.entrySet().stream()
                            .collect(Collectors.toMap(kv -> kv.getKey().toString(), Map.Entry::getValue)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            status = ConsumerGroupResetOffsetStatus.ofFailure(e.getMessage());
        }

        consumerGroupResetOffsets.setStatus(status);
        return consumerGroupResetOffsets;
    }
}
