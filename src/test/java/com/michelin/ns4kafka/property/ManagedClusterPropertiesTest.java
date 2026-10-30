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
package com.michelin.ns4kafka.property;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.michelin.ns4kafka.repository.kafka.DelayStartupListener;
import com.michelin.ns4kafka.service.executor.KafkaAsyncExecutorScheduler;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ManagedClusterPropertiesTest {
    @Test
    void shouldBindTableflowCredentialsSeparatelyForEachCluster() {
        String first = "ns4kafka.managed-clusters.first.confluent-cloud.";
        String second = "ns4kafka.managed-clusters.second.confluent-cloud.";
        try (var context = ApplicationContext.builder()
                .enableDefaultPropertySources(false)
                .properties(Map.of(
                        first + "basic-auth-username", "management-key",
                        first + "tableflow.basic-auth-username", "first-tableflow-key",
                        first + "tableflow.basic-auth-password", "first-tableflow-secret",
                        second + "tableflow.basic-auth-username", "second-tableflow-key",
                        second + "tableflow.basic-auth-password", "second-tableflow-secret"))
                .singletons(mock(DelayStartupListener.class), mock(KafkaAsyncExecutorScheduler.class))
                .start()) {
            var firstConfig = context.getBean(ManagedClusterProperties.class, Qualifiers.byName("first"))
                    .getConfluentCloud();
            var secondConfig = context.getBean(ManagedClusterProperties.class, Qualifiers.byName("second"))
                    .getConfluentCloud();

            assertEquals("management-key", firstConfig.getBasicAuthUsername());
            assertEquals("first-tableflow-key", firstConfig.getTableflow().getBasicAuthUsername());
            assertEquals("first-tableflow-secret", firstConfig.getTableflow().getBasicAuthPassword());
            assertEquals("second-tableflow-key", secondConfig.getTableflow().getBasicAuthUsername());
            assertEquals("second-tableflow-secret", secondConfig.getTableflow().getBasicAuthPassword());
        }
    }
}
