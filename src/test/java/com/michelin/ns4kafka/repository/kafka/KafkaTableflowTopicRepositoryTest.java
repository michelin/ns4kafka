/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.repository.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import io.micronaut.json.JsonMapper;
import io.micronaut.scheduling.TaskScheduler;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;

class KafkaTableflowTopicRepositoryTest {
    private static KafkaTableflowTopicRepository repository() {
        return new KafkaTableflowTopicRepository(
                "ns4kafka.tableflow-topics",
                mock(Producer.class),
                mock(AdminClient.class),
                new Ns4KafkaProperties(),
                mock(TaskScheduler.class));
    }

    private static TableflowTopic topic(String cluster) {
        return TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name("orders")
                        .cluster(cluster)
                        .namespace("sales")
                        .generation(0)
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(List.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();
    }

    @Test
    void shouldRestorePendingConfigurationsFromKafkaAfterRestart() throws Exception {
        var original = repository();
        TableflowTopic desired = topic("cloud");
        String key = original.getMessageKey(desired);
        assertEquals("cloud/orders", key);
        var mapper = JsonMapper.createDefault();
        TableflowTopic restored = mapper.readValue(mapper.writeValueAsBytes(desired), TableflowTopic.class);
        var restarted = repository();
        restarted.receive(new ConsumerRecord<>("ns4kafka.tableflow-topics", 0, 0, key, restored));

        assertEquals(desired, restarted.findByName("cloud", "orders").orElseThrow());
        assertTrue(restarted.findByName("cloud", "orders").orElseThrow().isPending());
    }

    @Test
    void shouldKeepStoredConfigurationsIsolatedByCluster() {
        var repository = repository();
        TableflowTopic first = topic("first");
        TableflowTopic second = topic("second");
        repository.receive(new ConsumerRecord<>("ns4kafka.tableflow-topics", 0, 0, "first/orders", first));
        repository.receive(new ConsumerRecord<>("ns4kafka.tableflow-topics", 0, 1, "second/orders", second));
        assertEquals(List.of(first), repository.findAllForCluster("first"));
        assertEquals(List.of(second), repository.findAllForCluster("second"));
        assertTrue(repository.findAllForCluster("unknown").isEmpty());
        assertEquals(second, repository.findByName("second", "orders").orElseThrow());
    }
}
