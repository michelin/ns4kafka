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

import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.GROUP;
import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TOPIC;
import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TRANSACTIONAL_ID;
import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_MANAGE;
import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_READ;
import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingRequest;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingResponse;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ConfluentRoleBindingAsyncExecutorTest {
    private static final Instant instant = Instant.parse("2026-01-01T00:00:00Z");

    @Mock
    ConfluentCloudClient confluentCloudClient;

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @InjectMocks
    ConfluentRoleBindingAsyncExecutor rbAsyncExecutor;

    @Mock
    AclService aclService;

    @Mock
    StreamService streamService;

    @Mock
    NamespaceRepository namespaceRepository;

    @Test
    void shouldConvertTopicAclToRoleBinding() {
        AccessControlEntry ownerAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry readAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-read")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry writeAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.TOPIC, "ns1-*");
        RoleBinding writeRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_WRITE, AccessControlEntry.ResourceType.TOPIC, "ns1-*");

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        List<RoleBinding> ownerRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(ownerAcl);
        List<RoleBinding> readRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(readAcl);
        List<RoleBinding> writeRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(writeAcl);

        assertEquals(2, ownerRoleBindings.size());
        assertTrue(ownerRoleBindings.containsAll(List.of(readRoleBinding, writeRoleBinding)));
        assertEquals(1, readRoleBindings.size());
        assertTrue(readRoleBindings.contains(readRoleBinding));
        assertEquals(1, writeRoleBindings.size());
        assertTrue(writeRoleBindings.contains(writeRoleBinding));
    }

    @Test
    void shouldConvertConnectorAclToRoleBinding() {
        AccessControlEntry ownerAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readGroupRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, GROUP, "connect-ns1-*");

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        List<RoleBinding> ownerRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(ownerAcl);

        assertEquals(1, ownerRoleBindings.size());
        assertTrue(ownerRoleBindings.contains(readGroupRoleBinding));
    }

    @Test
    void shouldNotConvertNonOwnerConnectorAclToRoleBinding() {
        AccessControlEntry writeAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry readAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertEquals(List.of(), rbAsyncExecutor.convertAclToRoleBinding(writeAcl));
        assertEquals(List.of(), rbAsyncExecutor.convertAclToRoleBinding(readAcl));
    }

    @Test
    void shouldConvertGroupAclToRoleBinding() {
        AccessControlEntry ownerAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(GROUP)
                        .resource("ns1-group")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readGroupRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, GROUP, "ns1-group");

        Namespace ns = Namespace.builder()
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(ns));

        List<RoleBinding> ownerRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(ownerAcl);

        assertEquals(1, ownerRoleBindings.size());
        assertTrue(ownerRoleBindings.contains(readGroupRoleBinding));
    }

    @Test
    void shouldNotConvertWriteGroupAclToRoleBinding() {
        AccessControlEntry writeAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(GROUP)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertEquals(List.of(), rbAsyncExecutor.convertAclToRoleBinding(writeAcl));
    }

    @Test
    void shouldConvertTransactionalIdAclToRoleBinding() {
        AccessControlEntry ownerAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TRANSACTIONAL_ID)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readGroupRoleBinding = new RoleBinding("User:user1", DEVELOPER_WRITE, TRANSACTIONAL_ID, "ns1-*");

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        List<RoleBinding> ownerRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(ownerAcl);

        assertEquals(1, ownerRoleBindings.size());
        assertTrue(ownerRoleBindings.contains(readGroupRoleBinding));
    }

    @Test
    void shouldNotConvertReadTransactionalIdAclToRoleBinding() {
        AccessControlEntry readAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TRANSACTIONAL_ID)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertEquals(List.of(), rbAsyncExecutor.convertAclToRoleBinding(readAcl));
    }

    @Test
    void shouldNotConvertOtherTypeAclToRoleBinding() {
        AccessControlEntry connectClusterAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        assertTrue(rbAsyncExecutor.convertAclToRoleBinding(connectClusterAcl).isEmpty());
    }

    @Test
    void shouldConvertKafkaStreamsToRoleBinding() {
        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .namespace("ns1")
                        .name("ns1-stream")
                        .build())
                .build();

        RoleBinding readGroupRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertEquals(readGroupRoleBinding, rbAsyncExecutor.convertKafkaStreamsToRoleBinding(kafkaStream));
    }

    @Test
    void shouldDeleteAcls() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-read")
                        .namespace("ns1")
                        .generation(1)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry emptyResponseAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-read-empty")
                        .namespace("ns1")
                        .generation(1)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns-empty")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.TOPIC, "ns1-*");
        RoleBinding readEmptyRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.TOPIC, "ns-empty*");
        RoleBindingRequest readRequest = new RoleBindingRequest(readRoleBinding, buildConfluentCloudProperties());
        RoleBindingRequest readEmptyRequest =
                new RoleBindingRequest(readEmptyRoleBinding, buildConfluentCloudProperties());

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(buildConfluentCloudProperties());
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.listRoleBindings("cluster", readRequest.crnPattern()))
                .thenReturn(Flux.just(
                        // Same resource, but other principal or other role
                        toResponse("rb-other-user", new RoleBinding("User:user2", DEVELOPER_READ, TOPIC, "ns1-*")),
                        toResponse("rb-other-role", new RoleBinding("User:user1", DEVELOPER_WRITE, TOPIC, "ns1-*")),
                        toResponse("rb-match", readRoleBinding)));
        when(confluentCloudClient.listRoleBindings("cluster", readEmptyRequest.crnPattern()))
                .thenReturn(Flux.empty());
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-match"))
                .thenReturn(Mono.just(toResponse("rb-match", readRoleBinding)));

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl, emptyResponseAcl));

        verify(confluentCloudClient).deleteRoleBinding("cluster", "rb-match");
        verify(confluentCloudClient, times(1)).deleteRoleBinding(any(), anyString());
        verify(aclService, never()).create(any());
    }

    @Test
    void shouldNotFailWhenErrorDeletingAcl() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-write")
                        .namespace("ns1")
                        .generation(1)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding writeRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_WRITE, AccessControlEntry.ResourceType.TOPIC, "ns1-*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(managedClusterProperties.getConfluentCloud()).thenReturn(buildConfluentCloudProperties());
        when(confluentCloudClient.listRoleBindings(
                        "cluster",
                        new RoleBindingRequest(writeRoleBinding, buildConfluentCloudProperties()).crnPattern()))
                .thenReturn(Flux.just(toResponse("rb-write", writeRoleBinding)));
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-write"))
                .thenReturn(Mono.error(new RuntimeException("error")));

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl));

        verify(aclService, never()).create(any());
    }

    @Test
    void shouldDeleteKafkaStreams() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .generation(1)
                        .build())
                .build();

        KafkaStream emptyResponseKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream-empty")
                        .generation(1)
                        .build())
                .build();

        RoleBinding manageTopicRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");
        RoleBinding manageTopicRoleBindingEmpty = new RoleBinding(
                "User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream-empty*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(buildConfluentCloudProperties());
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.listRoleBindings(
                        "cluster",
                        new RoleBindingRequest(manageTopicRoleBinding, buildConfluentCloudProperties()).crnPattern()))
                .thenReturn(Flux.just(toResponse("rb-manage", manageTopicRoleBinding)));
        when(confluentCloudClient.listRoleBindings(
                        "cluster",
                        new RoleBindingRequest(manageTopicRoleBindingEmpty, buildConfluentCloudProperties())
                                .crnPattern()))
                .thenReturn(Flux.empty());
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-manage"))
                .thenReturn(Mono.just(toResponse("rb-manage", manageTopicRoleBinding)));

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream, emptyResponseKafkaStream));

        verify(confluentCloudClient).deleteRoleBinding("cluster", "rb-manage");
        verify(confluentCloudClient, times(1)).deleteRoleBinding(any(), anyString());
        verify(streamService, never()).create(any());
    }

    @Test
    void shouldNotFailWhenErrorDeletingKafkaStream() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .generation(1)
                        .build())
                .build();

        RoleBinding manageTopicRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(managedClusterProperties.getConfluentCloud()).thenReturn(buildConfluentCloudProperties());
        when(confluentCloudClient.listRoleBindings(
                        "cluster",
                        new RoleBindingRequest(manageTopicRoleBinding, buildConfluentCloudProperties()).crnPattern()))
                .thenReturn(Flux.just(toResponse("rb-manage", manageTopicRoleBinding)));
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-manage"))
                .thenReturn(Mono.error(new RuntimeException("error")));

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(streamService, never()).create(any());
    }

    @Test
    void shouldCreateMissingAndDeleteUnsynchronizedRoleBindings() {
        AccessControlEntry acl =
                buildAcl("ns1-acl", AccessControlEntry.Permission.OWNER, Resource.Metadata.Status.ofPending());
        KafkaStream kafkaStream = buildKafkaStream(Resource.Metadata.Status.ofSuccess());
        RoleBinding readRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*");
        RoleBinding writeRoleBinding = new RoleBinding("User:user1", DEVELOPER_WRITE, TOPIC, "ns1-*");
        RoleBinding manageRoleBinding = new RoleBinding("User:user1", DEVELOPER_MANAGE, TOPIC, "ns1-stream*");
        RoleBindingResponse unsyncRoleBinding =
                toResponse("rb-unsync", new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-old*"));

        stubSynchronization(
                true,
                List.of(
                        toResponse("rb-read", readRoleBinding),
                        toResponse("rb-manage", manageRoleBinding),
                        unsyncRoleBinding,
                        // Not managed by Ns4Kafka: other role or other principal
                        RoleBindingResponse.builder()
                                .id("rb-owner")
                                .principal("User:user1")
                                .roleName("ResourceOwner")
                                .crnPattern(RoleBindingRequest.clusterCrnPattern(buildConfluentCloudProperties())
                                        + "topic=*")
                                .build(),
                        toResponse("rb-other", new RoleBinding("User:other", DEVELOPER_READ, TOPIC, "other-*"))),
                List.of(acl),
                List.of(kafkaStream));
        when(confluentCloudClient.createRoleBinding("cluster", writeRoleBinding))
                .thenReturn(
                        Mono.just(RoleBindingResponse.builder().id("rb-write").build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-unsync")).thenReturn(Mono.just(unsyncRoleBinding));
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient).createRoleBinding("cluster", writeRoleBinding);
        verify(confluentCloudClient, times(1)).createRoleBinding(any(), any());
        verify(confluentCloudClient).deleteRoleBinding("cluster", "rb-unsync");
        verify(confluentCloudClient, times(1)).deleteRoleBinding(any(), anyString());
        verify(aclService)
                .create(argThat(
                        a -> a == acl && a.isSuccess() && a.getMetadata().getGeneration() == 1));
        verify(streamService, never()).create(any());
    }

    @Test
    void shouldContinueCreatingAndDeletingAfterErrors() {
        AccessControlEntry acl =
                buildAcl("ns1-acl", AccessControlEntry.Permission.OWNER, Resource.Metadata.Status.ofPending());
        RoleBinding readRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*");
        RoleBinding writeRoleBinding = new RoleBinding("User:user1", DEVELOPER_WRITE, TOPIC, "ns1-*");

        stubSynchronization(
                true,
                List.of(
                        toResponse("rb-unsync-1", new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "old1-*")),
                        toResponse("rb-unsync-2", new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "old2-*"))),
                List.of(acl),
                List.of());
        when(confluentCloudClient.createRoleBinding("cluster", readRoleBinding))
                .thenReturn(Mono.error(new RuntimeException("read error")));
        when(confluentCloudClient.createRoleBinding("cluster", writeRoleBinding))
                .thenReturn(
                        Mono.just(RoleBindingResponse.builder().id("rb-write").build()));
        when(confluentCloudClient.deleteRoleBinding(any(), anyString()))
                .thenReturn(Mono.error(new RuntimeException("delete error")));
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient).createRoleBinding("cluster", readRoleBinding);
        verify(confluentCloudClient).createRoleBinding("cluster", writeRoleBinding);
        verify(confluentCloudClient).deleteRoleBinding("cluster", "rb-unsync-1");
        verify(confluentCloudClient).deleteRoleBinding("cluster", "rb-unsync-2");
        verify(aclService)
                .create(argThat(a -> a == acl
                        && a.isFailed()
                        && "read error".equals(a.getMetadata().getStatus().getMessage())));
    }

    @Test
    void shouldNotDeleteUnsynchronizedRoleBindingsWhenDropUnsyncDisabled() {
        stubSynchronization(
                false,
                List.of(toResponse("rb-unsync", new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-old*"))),
                List.of(),
                List.of());

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, never()).deleteRoleBinding(any(), anyString());
        verify(confluentCloudClient, never()).createRoleBinding(any(), any());
    }

    @ParameterizedTest
    @MethodSource("unresolvedStatuses")
    void shouldUpdateStatusWhenRoleBindingsAlreadyExist(Resource.Metadata.Status status) {
        AccessControlEntry acl = buildAcl("ns1-acl", AccessControlEntry.Permission.READ, status);
        KafkaStream kafkaStream = buildKafkaStream(status);
        Namespace namespace = buildNamespace();

        stubSynchronization(
                true,
                List.of(
                        toResponse("rb-read", new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*")),
                        toResponse("rb-manage", new RoleBinding("User:user1", DEVELOPER_MANAGE, TOPIC, "ns1-stream*"))),
                List.of(acl),
                List.of(kafkaStream));
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(kafkaStream));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, never()).createRoleBinding(any(), any());
        verify(confluentCloudClient, never()).deleteRoleBinding(any(), anyString());
        verify(aclService)
                .create(argThat(
                        a -> a == acl && a.isSuccess() && a.getMetadata().getGeneration() == 1));
        verify(streamService)
                .create(argThat(ks ->
                        ks == kafkaStream && ks.isSuccess() && ks.getMetadata().getGeneration() == 1));
    }

    @Test
    void shouldCreateSharedRoleBindingOnce() {
        AccessControlEntry acl1 =
                buildAcl("ns1-acl", AccessControlEntry.Permission.READ, Resource.Metadata.Status.ofPending());
        AccessControlEntry acl2 =
                buildAcl("ns1-acl-2", AccessControlEntry.Permission.READ, Resource.Metadata.Status.ofPending());
        RoleBinding readRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*");

        stubSynchronization(true, List.of(), List.of(acl1, acl2), List.of());
        when(confluentCloudClient.createRoleBinding("cluster", readRoleBinding))
                .thenReturn(
                        Mono.just(RoleBindingResponse.builder().id("rb-read").build()));
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl1));
        when(aclService.findByName("ns1", "ns1-acl-2")).thenReturn(Optional.of(acl2));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, times(1)).createRoleBinding("cluster", readRoleBinding);
        verify(aclService).create(argThat(a -> a == acl1 && a.isSuccess()));
        verify(aclService).create(argThat(a -> a == acl2 && a.isSuccess()));
    }

    @Test
    void shouldFailKafkaStreamWhenRoleBindingCreationFails() {
        KafkaStream kafkaStream = buildKafkaStream(Resource.Metadata.Status.ofPending());

        stubSynchronization(true, List.of(), List.of(), List.of(kafkaStream));
        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(streamService.findByName(buildNamespace(), "ns1-stream")).thenReturn(Optional.of(kafkaStream));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(streamService)
                .create(argThat(ks -> ks == kafkaStream
                        && ks.isFailed()
                        && !ks.isCreated()
                        && "error".equals(ks.getMetadata().getStatus().getMessage())));
    }

    @Test
    void shouldSkipResourcesOfDeletedNamespaces() {
        AccessControlEntry acl =
                buildAcl("ns1-acl", AccessControlEntry.Permission.READ, Resource.Metadata.Status.ofPending());
        AccessControlEntry orphanAcl =
                buildAcl("ns1-orphan-acl", AccessControlEntry.Permission.WRITE, Resource.Metadata.Status.ofPending());
        orphanAcl.getSpec().setGrantedTo("deleted-namespace");
        KafkaStream orphanKafkaStream = buildKafkaStream(Resource.Metadata.Status.ofPending());
        orphanKafkaStream.getMetadata().setNamespace("deleted-namespace");
        RoleBinding readRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*");

        stubSynchronization(true, List.of(), List.of(acl, orphanAcl), List.of(orphanKafkaStream));
        when(confluentCloudClient.createRoleBinding("cluster", readRoleBinding))
                .thenReturn(
                        Mono.just(RoleBindingResponse.builder().id("rb-read").build()));
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, times(1)).createRoleBinding(any(), any());
        verify(aclService).create(argThat(a -> a == acl && a.isSuccess()));
        verify(aclService, never()).findByName("ns1", "ns1-orphan-acl");
        verify(streamService, never()).findByName(any(), any());
    }

    @Test
    void shouldNotPersistWhenSuccessAndRoleBindingsAlreadyExist() {
        AccessControlEntry acl =
                buildAcl("ns1-acl", AccessControlEntry.Permission.READ, Resource.Metadata.Status.ofSuccess());

        stubSynchronization(
                true,
                List.of(toResponse("rb-read", new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*"))),
                List.of(acl),
                List.of());

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, never()).createRoleBinding(any(), any());
        verify(aclService, never()).findByName(any(), any());
        verify(aclService, never()).create(any());
    }

    @ParameterizedTest
    @CsvSource(
            value = {"deleted, false", "reapplied, false", "nullStoredTimestamp, true", "nullReadTimestamp, false"},
            nullValues = "null")
    void shouldPersistOnlyWhenUnchangedSinceLastApply(String scenario, boolean shouldPersist) {
        AccessControlEntry acl =
                buildAcl("ns1-acl", AccessControlEntry.Permission.WRITE, Resource.Metadata.Status.ofPending());
        AccessControlEntry storedAcl =
                buildAcl("ns1-acl", AccessControlEntry.Permission.WRITE, Resource.Metadata.Status.ofPending());

        switch (scenario) {
            case "reapplied" -> storedAcl.getMetadata().setUpdateTimestamp(Date.from(instant.plusSeconds(1)));
            case "nullStoredTimestamp" -> storedAcl.getMetadata().setUpdateTimestamp(null);
            case "nullReadTimestamp" -> acl.getMetadata().setUpdateTimestamp(null);
            default -> {
                // Deleted
            }
        }

        stubSynchronization(true, List.of(), List.of(acl), List.of());
        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.just(RoleBindingResponse.builder().build()));
        when(aclService.findByName("ns1", "ns1-acl"))
                .thenReturn("deleted".equals(scenario) ? Optional.empty() : Optional.of(storedAcl));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(aclService, times(shouldPersist ? 1 : 0)).create(argThat(a -> a == acl && a.isSuccess()));
    }

    @Test
    void shouldNotSynchronizeRoleBindingsWhenListingFails() {
        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(buildConfluentCloudProperties());
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(buildNamespace()));
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(buildConfluentCloudProperties()) + "*"))
                .thenReturn(Flux.error(new RuntimeException("error")));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, never()).createRoleBinding(any(), any());
        verify(confluentCloudClient, never()).deleteRoleBinding(any(), anyString());
        verify(aclService, never()).create(any());
        verify(streamService, never()).create(any());
    }

    @Test
    void shouldSynchronizeRoleBindingsWhenClusterManagesRbac() {
        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(buildConfluentCloudProperties());
        when(managedClusterProperties.isManageAcls()).thenReturn(false);
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
        when(managedClusterProperties.isManageRbac()).thenReturn(true);
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(buildNamespace()));
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(buildConfluentCloudProperties()) + "*"))
                .thenReturn(Flux.empty());

        rbAsyncExecutor.run().block();

        verify(confluentCloudClient)
                .listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(buildConfluentCloudProperties()) + "*");
    }

    @Test
    void shouldNotSynchronizeRoleBindingsWhenClusterManagesAcls() {
        when(managedClusterProperties.isManageAcls()).thenReturn(true);

        rbAsyncExecutor.run().block();

        verify(confluentCloudClient, never()).listRoleBindings(any(), any());
    }

    @Test
    void shouldCreateCrnPattern() {
        ManagedClusterProperties.ConfluentCloudProperties properties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        properties.setOrganizationId("orgId");
        properties.setEnvironmentId("envId");
        properties.setClusterId("clusterId");

        RoleBinding topicRoleBinding = new RoleBinding("User:user", DEVELOPER_READ, TOPIC, "myTopic");
        RoleBindingRequest topicRbRequest = new RoleBindingRequest(topicRoleBinding, properties);
        String topicCrnPattern =
                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=myTopic";

        RoleBinding groupRoleBinding = new RoleBinding("User:user", DEVELOPER_READ, GROUP, "myGroup");
        RoleBindingRequest groupRbRequest = new RoleBindingRequest(groupRoleBinding, properties);
        String groupCrnPattern =
                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/group=myGroup";

        RoleBinding transIdRoleBinding = new RoleBinding("User:user", DEVELOPER_READ, TRANSACTIONAL_ID, "myTransId");
        RoleBindingRequest transIdRbRequest = new RoleBindingRequest(transIdRoleBinding, properties);
        String transIdCrnPattern =
                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/transactional-id=myTransId";

        assertEquals(topicCrnPattern, topicRbRequest.crnPattern());
        assertEquals(groupCrnPattern, groupRbRequest.crnPattern());
        assertEquals(transIdCrnPattern, transIdRbRequest.crnPattern());
    }

    static Stream<Resource.Metadata.Status> unresolvedStatuses() {
        return Stream.of(Resource.Metadata.Status.ofPending(), Resource.Metadata.Status.ofFailed("error"), null);
    }

    private static ManagedClusterProperties.ConfluentCloudProperties buildConfluentCloudProperties() {
        ManagedClusterProperties.ConfluentCloudProperties properties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        properties.setOrganizationId("orgId");
        properties.setEnvironmentId("envId");
        properties.setClusterId("clusterId");
        return properties;
    }

    private static Namespace buildNamespace() {
        return Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();
    }

    private static RoleBindingResponse toResponse(String id, RoleBinding roleBinding) {
        RoleBindingRequest request = new RoleBindingRequest(roleBinding, buildConfluentCloudProperties());
        return RoleBindingResponse.builder()
                .id(id)
                .principal(request.principal())
                .roleName(request.roleName())
                .crnPattern(request.crnPattern())
                .build();
    }

    private void stubSynchronization(
            boolean dropUnsyncAcls,
            List<RoleBindingResponse> brokerRoleBindings,
            List<AccessControlEntry> acls,
            List<KafkaStream> kafkaStreams) {
        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(buildConfluentCloudProperties());
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(dropUnsyncAcls);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(buildConfluentCloudProperties()) + "*"))
                .thenReturn(Flux.fromIterable(brokerRoleBindings));
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(acls);
        when(streamService.findAllForCluster("cluster")).thenReturn(kafkaStreams);
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(buildNamespace()));

        if (!acls.isEmpty() || !kafkaStreams.isEmpty()) {
            when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(buildNamespace()));
        }
    }

    private static AccessControlEntry buildAcl(
            String name, AccessControlEntry.Permission permission, Resource.Metadata.Status status) {
        return AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name(name)
                        .namespace("ns1")
                        .status(status)
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(permission)
                        .grantedTo("ns1")
                        .build())
                .build();
    }

    private static KafkaStream buildKafkaStream(Resource.Metadata.Status status) {
        return KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(status)
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .build();
    }
}
