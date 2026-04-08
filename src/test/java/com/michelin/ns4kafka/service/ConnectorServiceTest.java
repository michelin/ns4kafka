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
package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.service.client.connect.entities.ConnectorType.SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.connect.ConnectCluster;
import com.michelin.ns4kafka.model.connect.Connector;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient;
import com.michelin.ns4kafka.service.client.connect.entities.ConfigInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConfigInfos;
import com.michelin.ns4kafka.service.client.connect.entities.ConfigKeyInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConfigValueInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorPluginInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorStateInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorStatus;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorType;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.ResourceValidator;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
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
class ConnectorServiceTest {
    @Mock
    AclService aclService;

    @Mock
    KafkaConnectClient kafkaConnectClient;

    @Mock
    ConnectorRepository connectorRepository;

    @InjectMocks
    ConnectorService connectorService;

    @Mock
    ConnectClusterService connectClusterService;

    @Test
    void shouldListConnectorsWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of());

        assertTrue(connectorService.findAllForNamespace(ns).isEmpty());
    }

    @Test
    void shouldFindAllForNamespace() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector c1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .build();

        Connector c2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect2").build())
                .build();

        Connector c3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect1").build())
                .build();

        Connector c4 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect2").build())
                .build();

        Connector c5 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns2-connect1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("other-connect1")
                                .build())
                        .build());

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);

        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(c1, c2, c3, c4, c5));
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect2")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect2")).thenReturn(false);
        when(aclService.isResourceCoveredByAcls(acls, "ns2-connect1")).thenReturn(false);

        assertEquals(List.of(c1, c2, c3), connectorService.findAllForNamespace(ns));
    }

    @Test
    void shouldFindConnectorsWithWildcardName() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector c1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .build();

        Connector c2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect1").build())
                .build();

        Connector c3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect2").build())
                .build();

        Connector c4 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns2-connect1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("other-connect1")
                                .build())
                        .build());

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);

        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(c1, c2, c3, c4));
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect2")).thenReturn(false);
        when(aclService.isResourceCoveredByAcls(acls, "ns2-connect1")).thenReturn(false);

        assertEquals(List.of(c1, c2), connectorService.findByWildcardName(ns, "*"));
    }

    @Test
    void shouldFindConnectorsWithNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector c1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .build();

        Connector c2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect1").build())
                .build();

        Connector c3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect2").build())
                .build();

        Connector c4 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns2-connect1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("other-connect1")
                                .build())
                        .build());

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);

        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(c1, c2, c3, c4));
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect2")).thenReturn(false);
        when(aclService.isResourceCoveredByAcls(acls, "ns2-connect1")).thenReturn(false);

        assertEquals(List.of(c1), connectorService.findByWildcardName(ns, "ns-connect1"));
        assertEquals(List.of(c2), connectorService.findByWildcardName(ns, "other-connect1"));
        assertTrue(connectorService.findByWildcardName(ns, "ns2-connect1").isEmpty());
        assertTrue(connectorService.findByWildcardName(ns, "ns4-connect1").isEmpty());
    }

    @Test
    void shouldFindConnectorWithWildcardNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector c1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .build();
        Connector c2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect2").build())
                .build();
        Connector c3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect1").build())
                .build();
        Connector c4 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect2").build())
                .build();
        Connector c5 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns2-connect1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("other-")
                                .build())
                        .build());

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);

        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(c1, c2, c3, c4, c5));
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect2")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect2")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns2-connect1")).thenReturn(false);

        assertEquals(List.of(c1, c2), connectorService.findByWildcardName(ns, "ns-connect?"));
        assertEquals(List.of(c1, c3), connectorService.findByWildcardName(ns, "*-connect1"));
        assertEquals(List.of(c1, c2, c3, c4), connectorService.findByWildcardName(ns, "*-connect?"));
        assertTrue(connectorService.findByWildcardName(ns, "ns2-*").isEmpty());
        assertTrue(connectorService.findByWildcardName(ns, "ns*4-connect?").isEmpty());
    }

    @Test
    void shouldNotFindByName() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of());

        assertTrue(connectorService.findByName(ns, "ns-connect1").isEmpty());
    }

    @Test
    void shouldFindByName() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Connector c1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .build();

        Connector c2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect2").build())
                .build();

        Connector c3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("other-connect1")
                                .build())
                        .build());

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(c1, c2, c3));
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect2")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "other-connect1")).thenReturn(true);

        Optional<Connector> actual = connectorService.findByName(ns, "ns-connect1");

        assertTrue(actual.isPresent());
        assertEquals("ns-connect1", actual.get().getMetadata().getName());
    }

    @Test
    void shouldFindAllByConnectCluster() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Connector c1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster")
                        .build())
                .build();

        Connector c2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect2").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster2")
                        .build())
                .build();

        Connector c3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster3")
                        .build())
                .build();

        Connector c4 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("other-connect2").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster4")
                        .build())
                .build();

        Connector c5 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns2-connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("connect-cluster5")
                        .build())
                .build();

        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(c1, c2, c3, c4, c5));

        List<Connector> actual = connectorService.findAllByConnectCluster(ns, "connect-cluster");

        assertEquals(1, actual.size());
        assertTrue(actual.stream()
                .anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
    }

    @Test
    void shouldNotValidateLocallyWhenInvalidConnectCluster() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("wrong")
                        .config(Map.of("connector.class", "Test"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .validationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .sinkValidationConstraints(Map.of())
                                .classValidationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(connectClusterService.findAllForNamespaceWithWritePermission(ns)).thenReturn(List.of());

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> {
                    assertEquals(1, response.size());
                    assertEquals(
                            "Invalid value \"wrong\" for field \"connectCluster\": value must be one of \"local-name\".",
                            response.getFirst());
                })
                .verifyComplete();
    }

    @Test
    void shouldNotValidateLocallyWhenNoClassName() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of())
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> {
                    assertEquals(1, response.size());
                    assertEquals(
                            "Invalid empty value for field \"connector.class\": value must not be null.",
                            response.getFirst());
                })
                .verifyComplete();
    }

    @Test
    void shouldNotValidateLocallyWhenInvalidClassName() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(kafkaConnectClient.connectPlugins("local", "local-name")).thenReturn(Mono.just(List.of()));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> {
                    assertEquals(1, response.size());
                    assertEquals(
                            "Invalid value \"org.apache.kafka.connect.file.FileStreamSinkConnector\" "
                                    + "for field \"connector.class\": failed to find any class that implements connector and "
                                    + "which name matches org.apache.kafka.connect.file.FileStreamSinkConnector.",
                            response.getFirst());
                })
                .verifyComplete();
    }

    @Test
    void shouldNotValidateLocallyWhenValidationErrors() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .validationConstraints(Map.of("missing.field", new ResourceValidator.NonEmptyString()))
                                .sinkValidationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .classValidationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(Mono.just(List.of(new ConnectorPluginInfo(
                        "org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> {
                    assertEquals(1, response.size());
                    assertEquals(
                            "Invalid empty value for field \"missing.field\": value must not be null.",
                            response.getFirst());
                })
                .verifyComplete();
    }

    @Test
    void shouldValidateLocally() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .classValidationConstraints(Map.of())
                                .sinkValidationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .validationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(Mono.just(List.of(new ConnectorPluginInfo(
                        "org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> assertTrue(response.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldValidateLocallyWhenConstraintNull() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(Mono.just(List.of(new ConnectorPluginInfo(
                        "org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> assertTrue(response.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldValidateLocallyWhenConstraintEmpty() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .classValidationConstraints(Map.of())
                                .sinkValidationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(Mono.just(List.of(new ConnectorPluginInfo(
                        "org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> assertTrue(response.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldValidateLocallyWhenSinkValidationConstraintNull() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .classValidationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .validationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(Mono.just(List.of(new ConnectorPluginInfo(
                        "org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> assertTrue(response.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldValidateLocallyOnSelfDeployedConnectCluster() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .classValidationConstraints(Map.of())
                                .sinkValidationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .validationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of())
                        .build())
                .build();

        when(connectClusterService.findAllForNamespaceWithWritePermission(ns))
                .thenReturn(List.of(ConnectCluster.builder()
                        .metadata(Resource.Metadata.builder().name("local-name").build())
                        .build()));

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(Mono.just(List.of(new ConnectorPluginInfo(
                        "org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
                .consumeNextWith(response -> assertTrue(response.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldNotValidateRemotelyWhenErrorHappens() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "com.michelin.NoClass"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ConfigInfos configInfos = new ConfigInfos(
                "name",
                1,
                List.of(),
                List.of(new ConfigInfo(
                        new ConfigKeyInfo(null, null, false, null, null, null, null, 0, null, null, null),
                        new ConfigValueInfo(null, null, null, List.of("error_message"), true))));

        when(kafkaConnectClient.validate(eq("local"), eq("local-name"), any(), any()))
                .thenReturn(Mono.just(configInfos));

        StepVerifier.create(connectorService.validateRemotely(ns, connector))
                .consumeNextWith(response -> {
                    assertEquals(1, response.size());
                    assertEquals("Invalid \"connect1\": error_message.", response.getFirst());
                })
                .verifyComplete();
    }

    @Test
    void shouldValidateRemotely() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ConfigInfos configInfos = new ConfigInfos("name", 1, List.of(), List.of());

        when(kafkaConnectClient.validate(eq("local"), eq("local-name"), any(), any()))
                .thenReturn(Mono.just(configInfos));

        StepVerifier.create(connectorService.validateRemotely(ns, connector))
                .consumeNextWith(response -> assertTrue(response.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldListUnsynchronizedConnectors() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns-connect-cluster")
                        .cluster("local")
                        .build())
                .build();

        ConnectorStatus c1 =
                new ConnectorStatus(new ConnectorInfo("ns-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c2 =
                new ConnectorStatus(new ConnectorInfo("ns-connect2", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c3 =
                new ConnectorStatus(new ConnectorInfo("ns1-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c4 =
                new ConnectorStatus(new ConnectorInfo("ns2-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c5 =
                new ConnectorStatus(new ConnectorInfo("ns1-connect2", Map.of(), List.of(), ConnectorType.SINK), null);

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns1-connect1")
                                .build())
                        .build());

        when(connectClusterService.findAllForNamespaceWithWritePermission(ns)).thenReturn(List.of(connectCluster));
        when(kafkaConnectClient.listAll("local", "local-name"))
                .thenReturn(Mono.just(
                        Map.of("ns-connect1", c1, "ns-connect2", c2, "ns1-connect1", c3, "ns2-connect1", c4)));
        when(kafkaConnectClient.listAll("local", "ns-connect-cluster"))
                .thenReturn(Mono.just(Map.of("ns1-connect2", c5)));

        // list of existing Ns4Kafka access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect2"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect2"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns2-connect1"))
                .thenReturn(false);

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);

        // no connects exists into Ns4Kafka
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of());

        StepVerifier.create(connectorService
                        .listUnsynchronizedConnectorsByWildcardName(ns, "*")
                        .collectList())
                .assertNext(connectors -> {
                    assertTrue(connectors.stream()
                            .anyMatch(connector ->
                                    connector.getMetadata().getName().equals("ns-connect1")));
                    assertTrue(connectors.stream()
                            .anyMatch(connector ->
                                    connector.getMetadata().getName().equals("ns-connect2")));
                    assertTrue(connectors.stream()
                            .anyMatch(connector ->
                                    connector.getMetadata().getName().equals("ns1-connect1")));
                    assertTrue(connectors.stream()
                            .anyMatch(connector ->
                                    connector.getMetadata().getName().equals("ns1-connect2")));
                })
                .verifyComplete();
    }

    @Test
    void shouldListUnsynchronizedConnectorsWhenAllExistingAlready() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect-cluster").build())
                .build();

        Connector co1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .build();

        Connector co2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect2").build())
                .build();

        Connector co3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns1-connect1").build())
                .build();

        Connector co4 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns2-connect1").build())
                .build();

        Connector co5 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns1-connect2").build())
                .build();

        ConnectorStatus c1 =
                new ConnectorStatus(new ConnectorInfo("ns-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c2 =
                new ConnectorStatus(new ConnectorInfo("ns-connect2", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c3 =
                new ConnectorStatus(new ConnectorInfo("ns1-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c4 =
                new ConnectorStatus(new ConnectorInfo("ns2-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c5 =
                new ConnectorStatus(new ConnectorInfo("ns1-connect2", Map.of(), List.of(), ConnectorType.SINK), null);

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns1-connect1")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns1-connect2")
                                .build())
                        .build());

        when(connectClusterService.findAllForNamespaceWithWritePermission(ns)).thenReturn(List.of(connectCluster));
        when(kafkaConnectClient.listAll("local", "local-name"))
                .thenReturn(Mono.just(
                        Map.of("ns-connect1", c1, "ns-connect2", c2, "ns1-connect1", c3, "ns2-connect1", c4)));
        when(kafkaConnectClient.listAll("local", "ns-connect-cluster"))
                .thenReturn(Mono.just(Map.of("ns1-connect2", c5)));
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(co1, co2, co3, co4, co5));

        // list of existing Ns4Kafka access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect2"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect2"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns2-connect1"))
                .thenReturn(false);

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect2")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns1-connect1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns1-connect2")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns2-connect1")).thenReturn(false);

        StepVerifier.create(connectorService.listUnsynchronizedConnectorsByWildcardName(ns, "*"))
                .verifyComplete();
    }

    @Test
    void shouldListUnsynchronizedConnectorsWhenNotAllExisting() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // list of existing broker connectors
        ConnectorStatus c1 =
                new ConnectorStatus(new ConnectorInfo("ns-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c2 =
                new ConnectorStatus(new ConnectorInfo("ns-connect2", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c3 =
                new ConnectorStatus(new ConnectorInfo("ns1-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c4 =
                new ConnectorStatus(new ConnectorInfo("ns2-connect1", Map.of(), List.of(), ConnectorType.SINK), null);

        Connector co1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .build();

        Connector co2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect2").build())
                .build();

        Connector co3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns1-connect1").build())
                .build();

        Connector co4 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns2-connect1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns1-connect1")
                                .build())
                        .build());

        when(kafkaConnectClient.listAll("local", "local-name"))
                .thenReturn(Mono.just(
                        Map.of("ns-connect1", c1, "ns-connect2", c2, "ns1-connect1", c3, "ns2-connect1", c4)));

        // list of existing broker connects
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(co1, co2, co3, co4));

        // list of existing Ns4Kafka access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect2"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns2-connect1"))
                .thenReturn(false);

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(co1));
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect1")).thenReturn(true);

        StepVerifier.create(connectorService
                        .listUnsynchronizedConnectorsByWildcardName(ns, "*")
                        .collectList())
                .assertNext(connectors -> {
                    assertTrue(connectors.stream()
                            .noneMatch(connector ->
                                    connector.getMetadata().getName().equals("ns-connect1")));
                    assertTrue(connectors.stream()
                            .anyMatch(connector ->
                                    connector.getMetadata().getName().equals("ns-connect2")));
                    assertTrue(connectors.stream()
                            .anyMatch(connector ->
                                    connector.getMetadata().getName().equals("ns1-connect1")));
                    assertTrue(connectors.stream()
                            .noneMatch(connector ->
                                    connector.getMetadata().getName().equals("ns1-connect2")));
                })
                .verifyComplete();
    }

    @Test
    void shouldListUnsynchronizedConnectorsWithNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // list of existing broker connectors
        ConnectorStatus c1 =
                new ConnectorStatus(new ConnectorInfo("ns-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c2 =
                new ConnectorStatus(new ConnectorInfo("ns-connect2", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c3 =
                new ConnectorStatus(new ConnectorInfo("ns1-connect1", Map.of(), List.of(), ConnectorType.SINK), null);
        ConnectorStatus c4 =
                new ConnectorStatus(new ConnectorInfo("ns2-connect1", Map.of(), List.of(), ConnectorType.SINK), null);

        Connector co1 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .build();

        Connector co2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect2").build())
                .build();

        Connector co3 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns1-connect1").build())
                .build();

        Connector co4 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns2-connect1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns1-connect1")
                                .build())
                        .build());

        when(kafkaConnectClient.listAll("local", "local-name"))
                .thenReturn(Mono.just(
                        Map.of("ns-connect1", c1, "ns-connect2", c2, "ns1-connect1", c3, "ns2-connect1", c4)));

        // list of existing broker connects
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(co1, co2, co3, co4));

        // list of existing Ns4Kafka access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect2"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace", AccessControlEntry.ResourceType.CONNECT, "ns2-connect1"))
                .thenReturn(false);

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.CONNECT))
                .thenReturn(acls);
        when(connectorRepository.findAllForCluster("local")).thenReturn(List.of(co1));
        when(aclService.isResourceCoveredByAcls(acls, "ns-connect1")).thenReturn(true);

        StepVerifier.create(connectorService.listUnsynchronizedConnectorsByWildcardName(ns, "ns-*"))
                .consumeNextWith(connector ->
                        assertEquals("ns-connect2", connector.getMetadata().getName()))
                .verifyComplete();
    }

    @Test
    void shouldRestartAllTasksOfConnector() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("ns-connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .build())
                .build();

        when(kafkaConnectClient.status(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName()))
                .thenReturn(Mono.just(new ConnectorStateInfo(
                        "connector",
                        new ConnectorStateInfo.ConnectorState("RUNNING", "worker", "message"),
                        List.of(
                                new ConnectorStateInfo.TaskState(0, "RUNNING", "worker", "message"),
                                new ConnectorStateInfo.TaskState(1, "RUNNING", "worker", "message"),
                                new ConnectorStateInfo.TaskState(2, "RUNNING", "worker", "message")),
                        SOURCE)));

        when(kafkaConnectClient.restart(any(), any(), any(), anyInt())).thenReturn(Mono.just(HttpResponse.ok()));

        StepVerifier.create(connectorService.restart(namespace, connector))
                .consumeNextWith(response -> assertEquals(HttpStatus.OK, response.getStatus()))
                .verifyComplete();

        verify(kafkaConnectClient)
                .restart(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName(),
                        0);
        verify(kafkaConnectClient)
                .restart(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName(),
                        1);
        verify(kafkaConnectClient)
                .restart(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName(),
                        2);
    }
}
