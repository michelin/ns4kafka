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
package com.michelin.ns4kafka.service.executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.connect.ConnectCluster;
import com.michelin.ns4kafka.model.connect.Connector;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.ConnectClusterRepository;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import com.michelin.ns4kafka.service.ConnectClusterService;
import com.michelin.ns4kafka.service.ConnectorService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorSpecs;
import io.micronaut.http.HttpResponse;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ConnectorAsyncExecutorTest {
    private static final Instant instant = Instant.parse("2026-01-01T00:00:00Z");

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @Mock
    ConnectorRepository connectorRepository;

    @Mock
    ConnectClusterRepository connectClusterRepository;

    @Mock
    KafkaConnectClient kafkaConnectClient;

    @Mock
    ConnectorService connectorService;

    @Mock
    ConnectClusterService connectClusterService;

    @Mock
    NamespaceService namespaceService;

    @InjectMocks
    ConnectorAsyncExecutor connectorAsyncExecutor;

    @Test
    void shouldDeployConnector() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.createOrUpdate(anyString(), anyString(), anyString(), any(ConnectorSpecs.class)))
                .thenReturn(Mono.just(new ConnectorInfo("connect1", Map.of(), List.of(), null)));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.empty());

        StepVerifier.create(connectorAsyncExecutor.run()).expectNextCount(1).verifyComplete();

        verify(connectorRepository).create(argThat(c -> c.equals(connector) && c.isSuccess() && c.isCreated()));
        verify(connectorRepository, never()).delete(any());
    }

    @Test
    void shouldDeployConnectorButNotUpdateStatusWhenChangedSinceLastApply() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        Connector newConnector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(0)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.createOrUpdate(anyString(), anyString(), anyString(), any(ConnectorSpecs.class)))
                .thenReturn(Mono.just(new ConnectorInfo("connect1", Map.of(), List.of(), null)));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.of(newConnector));

        StepVerifier.create(connectorAsyncExecutor.run()).expectNextCount(1).verifyComplete();

        verify(connectorRepository).create(argThat(c -> c.equals(newConnector) && c.isPending() && c.isCreated()));
        verify(connectorRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateConnectorWhenErrorCreating() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.createOrUpdate(anyString(), anyString(), anyString(), any(ConnectorSpecs.class)))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.empty());

        StepVerifier.create(connectorAsyncExecutor.run()).verifyError();

        verify(connectorRepository)
                .create(argThat(c -> c.isFailed()
                        && "error".equals(c.getMetadata().getStatus().getMessage())));
        verify(connectorRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateConnectorWhenErrorCreatingAndChangedSinceLastApply() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        Connector newConnector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(0)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.createOrUpdate(anyString(), anyString(), anyString(), any(ConnectorSpecs.class)))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.of(newConnector));

        StepVerifier.create(connectorAsyncExecutor.run()).verifyError();

        verify(connectorRepository, never()).create(any());
        verify(connectorRepository, never()).delete(any());
    }

    @Test
    void shouldDeleteConnector() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.delete("local", "connect-cluster", "connect1"))
                .thenReturn(Mono.just(HttpResponse.noContent()));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.empty());
        when(connectClusterRepository.findAllForCluster("local")).thenReturn(List.of());

        StepVerifier.create(connectorAsyncExecutor.run()).verifyComplete();

        verify(connectorRepository, never()).create(any());
        verify(connectorRepository).delete(connector);
    }

    @Test
    void shouldNotDeleteConnectorWhenChangedSinceLastApply() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        Connector newConnector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.delete("local", "connect-cluster", "connect1"))
                .thenReturn(Mono.just(HttpResponse.noContent()));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.of(newConnector));

        StepVerifier.create(connectorAsyncExecutor.run()).verifyComplete();

        verify(connectorRepository, never()).create(any());
        verify(connectorRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateConnectorWhenErrorDeleting() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.delete("local", "connect-cluster", "connect1"))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.empty());

        StepVerifier.create(connectorAsyncExecutor.run()).verifyError();

        verify(connectorRepository)
                .create(argThat(c -> c.isFailed()
                        && "error".equals(c.getMetadata().getStatus().getMessage())));
        verify(connectorRepository, never()).delete(any());
    }

    @Test
    void shouldNotDeleteConnectClusterWhenFailToDeleteConnector() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect-cluster")
                        .cluster("local")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://connect-cluster")
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.delete("local", "connect-cluster", "connect1"))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.empty());
        when(connectClusterRepository.findAllForCluster("local")).thenReturn(List.of(connectCluster));

        StepVerifier.create(connectorAsyncExecutor.run()).verifyError();

        verify(connectorRepository)
                .create(argThat(c -> c.getMetadata().getStatus().getPhase() == Resource.Metadata.Phase.FAIL));
        verify(connectClusterRepository)
                .create(argThat(cc -> cc.getMetadata().getStatus().getPhase() == Resource.Metadata.Phase.FAIL));
        verify(connectClusterService, never()).delete(any());
        verify(connectorRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateConnectorWhenErrorDeletingAndChangedSinceLastApply() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        Connector newConnector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(connector));
        when(kafkaConnectClient.delete("local", "connect-cluster", "connect1"))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.of(newConnector));

        StepVerifier.create(connectorAsyncExecutor.run()).verifyError();

        verify(connectorRepository, never()).create(any());
        verify(connectorRepository, never()).delete(any());
    }

    @Test
    void shouldNotDeleteConnectClusterWhenRemainingConnectors() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        Connector remainingConnector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect2")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofSuccess())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyOtherConnector"))
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect-cluster")
                        .cluster("local")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://connect-cluster")
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(connector))
                .thenReturn(List.of(remainingConnector));
        when(kafkaConnectClient.delete("local", "connect-cluster", "connect1"))
                .thenReturn(Mono.just(HttpResponse.noContent()));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.empty());
        when(connectClusterRepository.findAllForCluster("local")).thenReturn(List.of(connectCluster));

        StepVerifier.create(connectorAsyncExecutor.run()).verifyComplete();

        verify(connectorRepository).delete(connector);
        verify(connectClusterService, never()).delete(any());
    }

    @Test
    void shouldDeleteConnectClusterWhenNoRemainingConnectors() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect1")
                        .namespace("namespace")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .config(Map.of("connector.class", "io.connect.MyConnector"))
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connect-cluster")
                        .cluster("local")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://connect-cluster")
                        .build())
                .build();

        when(managedClusterProperties.isManageConnectors()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("local");
        when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(connector))
                .thenReturn(List.of());
        when(kafkaConnectClient.delete("local", "connect-cluster", "connect1"))
                .thenReturn(Mono.just(HttpResponse.noContent()));
        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(namespace));
        when(connectorService.findByName(namespace, "connect1")).thenReturn(Optional.empty());
        when(connectClusterRepository.findAllForCluster("local")).thenReturn(List.of(connectCluster));

        StepVerifier.create(connectorAsyncExecutor.run()).verifyComplete();

        verify(connectorRepository).delete(connector);
        verify(connectClusterService).delete(connectCluster);
    }
}
