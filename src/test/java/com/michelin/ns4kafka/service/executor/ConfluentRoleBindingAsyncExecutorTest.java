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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStreamRepository;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingRequest;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingResponse;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
    KafkaStreamRepository kafkaStreamRepository;

    @Mock
    AccessControlEntryRepository aclRepository;

    @Mock
    NamespaceService namespaceService;

    @Mock
    AclService aclService;

    @Mock
    StreamService streamService;

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

        when(namespaceService.findByName("ns1"))
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

        when(namespaceService.findByName("ns1"))
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
    void shouldNotConvertWrongConnectorAclToRoleBinding() {
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

        when(namespaceService.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertThrows(IllegalArgumentException.class, () -> rbAsyncExecutor.convertAclToRoleBinding(writeAcl));
        assertThrows(IllegalArgumentException.class, () -> rbAsyncExecutor.convertAclToRoleBinding(readAcl));
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

        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(ns));

        List<RoleBinding> ownerRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(ownerAcl);

        assertEquals(1, ownerRoleBindings.size());
        assertTrue(ownerRoleBindings.contains(readGroupRoleBinding));
    }

    @Test
    void shouldNotConvertWrongGroupAclToRoleBinding() {
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

        when(namespaceService.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertThrows(IllegalArgumentException.class, () -> rbAsyncExecutor.convertAclToRoleBinding(writeAcl));
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

        when(namespaceService.findByName("ns1"))
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
    void shouldNotConvertWrongTransactionalIdAclToRoleBinding() {
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

        when(namespaceService.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertThrows(IllegalArgumentException.class, () -> rbAsyncExecutor.convertAclToRoleBinding(readAcl));
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
    void shouldConvertKafkaStreamToRoleBinding() {
        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .namespace("ns1")
                        .name("ns1-stream")
                        .build())
                .build();

        RoleBinding readGroupRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");

        when(namespaceService.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertEquals(readGroupRoleBinding, rbAsyncExecutor.convertKafkaStreamToRoleBinding(kafkaStream));
    }

    @Test
    void shouldCreateAcl() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.empty());
        when(aclRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository).create(argThat(a -> a.equals(acl) && a.isSuccess() && a.isCreated()));
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldCreateAclButNotUpdateStatusWhenChangedSinceLastApply() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry newAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.of(newAcl));
        when(aclRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository).create(argThat(a -> a.equals(newAcl) && a.isPending() && a.isCreated()));
        verify(aclRepository, never()).delete(any());
    }

    @ParameterizedTest
    @CsvSource(
            value = {"1000, null, true", "null, 1000, false"},
            nullValues = "null")
    void shouldHandleNullableUpdateTimestampsWhenCreatingAcl(
            Long queuedTimestamp, Long latestTimestamp, boolean shouldMarkSuccess) {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(queuedTimestamp == null ? null : new Date(queuedTimestamp))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry latestAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(latestTimestamp == null ? null : new Date(latestTimestamp))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.of(latestAcl));
        when(aclRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository)
                .create(argThat(
                        a -> a == latestAcl && a.isCreated() && (shouldMarkSuccess ? a.isSuccess() : a.isPending())));
    }

    @ParameterizedTest
    @CsvSource(
            value = {"1000, null, true", "null, 1000, false"},
            nullValues = "null")
    void shouldHandleNullableUpdateTimestampsWhenCreatingKafkaStream(
            Long queuedTimestamp, Long latestTimestamp, boolean shouldMarkSuccess) {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(queuedTimestamp == null ? null : new Date(queuedTimestamp))
                        .generation(0)
                        .build())
                .build();

        KafkaStream latestKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(latestTimestamp == null ? null : new Date(latestTimestamp))
                        .generation(0)
                        .build())
                .build();

        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(latestKafkaStream));
        when(kafkaStreamRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository)
                .create(argThat(ks -> ks == latestKafkaStream
                        && ks.isCreated()
                        && (shouldMarkSuccess ? ks.isSuccess() : ks.isPending())));
    }

    @Test
    void shouldUpdateAclWhenErrorCreating() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.empty());
        when(aclRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository)
                .create(argThat(a -> a.isFailed()
                        && !a.isCreated()
                        && "error".equals(a.getMetadata().getStatus().getMessage())));
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateAclWhenErrorCreatingAndChangedSinceLastApply() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry newAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.of(newAcl));

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository, never()).create(any());
        verify(aclRepository, never()).delete(any());
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
        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.deleteRoleBinding("cluster", readRoleBinding)).thenReturn(Mono.just(response));
        when(confluentCloudClient.deleteRoleBinding("cluster", readEmptyRoleBinding))
                .thenReturn(Mono.empty());

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl, emptyResponseAcl));

        verify(confluentCloudClient).deleteRoleBinding("cluster", readRoleBinding);
        verify(confluentCloudClient).deleteRoleBinding("cluster", readEmptyRoleBinding);
        verify(aclRepository, never()).create(any());
        verify(aclRepository, never()).delete(any());
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
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.deleteRoleBinding("cluster", writeRoleBinding))
                .thenReturn(Mono.error(new RuntimeException("error")));

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository, never()).create(any());
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldCreateKafkaStream() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .build();

        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(namespaceService.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.empty());
        when(kafkaStreamRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository).create(argThat(ks -> ks.equals(kafkaStream) && ks.isSuccess() && ks.isCreated()));
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldCreateKafkaStreamButNotUpdateStatusWhenChangedSinceLastApply() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .build();

        KafkaStream newKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(0)
                        .build())
                .build();

        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(newKafkaStream));
        when(kafkaStreamRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository)
                .create(argThat(ks -> ks.equals(newKafkaStream) && ks.isPending() && ks.isCreated()));
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateKafkaStreamsWhenErrorCreating() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .build();

        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.empty());
        when(kafkaStreamRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository).create(argThat(ks -> ks.equals(kafkaStream) && ks.isFailed() && !ks.isCreated()));
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateKafkaStreamsWhenErrorCreatingAndChangedSinceLastApply() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .build();

        KafkaStream newKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(0)
                        .build())
                .build();

        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(newKafkaStream));

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository, never()).create(any());
        verify(kafkaStreamRepository, never()).delete(any());
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
        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.deleteRoleBinding("cluster", manageTopicRoleBinding))
                .thenReturn(Mono.just(response));
        when(confluentCloudClient.deleteRoleBinding("cluster", manageTopicRoleBindingEmpty))
                .thenReturn(Mono.empty());

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream, emptyResponseKafkaStream));

        verify(confluentCloudClient).deleteRoleBinding("cluster", manageTopicRoleBinding);
        verify(confluentCloudClient).deleteRoleBinding("cluster", manageTopicRoleBindingEmpty);
        verify(kafkaStreamRepository, never()).create(any());
        verify(kafkaStreamRepository, never()).delete(any());
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
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.deleteRoleBinding("cluster", manageTopicRoleBinding))
                .thenReturn(Mono.error(new RuntimeException("error")));

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository, never()).create(any());
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldSynchronizeRoleBindings() {
        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .cluster("cluster")
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();
        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.isManageAcls()).thenReturn(false);
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
        when(managedClusterProperties.isManageRbac()).thenReturn(true);
        when(aclService.findNonPublicToDeployForCluster("cluster")).thenReturn(List.of(acl));
        when(streamService.findAllToDeployForCluster("cluster")).thenReturn(List.of());
        when(namespaceService.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.empty());
        when(aclRepository.create(acl)).thenReturn(acl);

        rbAsyncExecutor.run();

        verify(confluentCloudClient).createRoleBinding(any(), any());
        verify(aclRepository).create(acl);
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
}
