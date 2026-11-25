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
package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.ns4kafka.util.enumation.Kind;
import io.micronaut.json.JsonMapper;
import io.micronaut.validation.validator.Validator;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TableflowTopicTest {
    @Test
    void shouldRequireAtLeastOneDayOfSnapshotRetention() {
        Validator validator = Validator.getInstance();
        Class<TableflowTopic.TableflowTopicSpec> spec = TableflowTopic.TableflowTopicSpec.class;
        assertEquals(
                1,
                validator
                        .validateValue(spec, "snapshotRetentionMs", 86_399_999L)
                        .size());
        assertEquals(
                0,
                validator
                        .validateValue(spec, "snapshotRetentionMs", 86_400_000L)
                        .size());
    }

    @Test
    void shouldDeserializeASeparateResourceReferencingAnExistingTopic() throws Exception {
        JsonMapper mapper = JsonMapper.createDefault();
        TableflowTopic resource = mapper.readValue("""
                {
                  "apiVersion": "v1",
                  "kind": "TableflowTopic",
                  "metadata": {"name": "orders"},
                  "spec": {"tableFormats": ["ICEBERG"], "storage": {"kind": "Managed"}}
                }
                """, TableflowTopic.class);

        assertEquals(Kind.TABLEFLOW_TOPIC, resource.getKind());
        assertEquals("v1", resource.getApiVersion());
        assertEquals("orders", resource.getMetadata().getName());
        assertEquals(Set.of("ICEBERG"), resource.getSpec().getTableFormats());
        assertEquals("Managed", resource.getSpec().getStorage().getKind());
        assertEquals(resource, mapper.readValue(mapper.writeValueAsBytes(resource), TableflowTopic.class));
    }

    @Test
    void shouldSerializeConfluentStatusSeparatelyFromTheResourceSpecification() throws Exception {
        JsonMapper mapper = JsonMapper.createDefault();
        TableflowTopic resource = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder().name("orders").build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .build())
                .status(TableflowTopic.TableflowTopicStatus.builder()
                        .id("tt-test")
                        .state(TableflowTopic.TableflowTopicState.builder()
                                .phase("PROVISIONING")
                                .errorMessage("")
                                .build())
                        .build())
                .build();

        TableflowTopic result = mapper.readValue(mapper.writeValueAsBytes(resource), TableflowTopic.class);

        assertEquals("tt-test", result.getStatus().getId());
        assertEquals("PROVISIONING", result.getStatus().getState().getPhase());
        assertEquals(resource.getStatus(), result.getStatus());
        result.setStatus(null);
        assertEquals(resource, result);
    }
}
