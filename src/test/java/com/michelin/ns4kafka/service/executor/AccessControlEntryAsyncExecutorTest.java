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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
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
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AccessControlEntryAsyncExecutorTest {
    private static final Instant INSTANT = Instant.parse("2026-01-01T00:00:00Z");
    private static final AclBinding READ_ACL_BINDING = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "ns1-", PatternType.PREFIXED),
            new org.apache.kafka.common.acl.AccessControlEntry(
                    "User:user1", "*", AclOperation.READ, AclPermissionType.ALLOW));
    private static final AclBinding STREAM_CREATE_ACL_BINDING = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "ns1-stream", PatternType.PREFIXED),
            new org.apache.kafka.common.acl.AccessControlEntry(
                    "User:user1", "*", AclOperation.CREATE, AclPermissionType.ALLOW));
    private static final AclBinding STREAM_DELETE_ACL_BINDING = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "ns1-stream", PatternType.PREFIXED),
            new org.apache.kafka.common.acl.AccessControlEntry(
                    "User:user1", "*", AclOperation.DELETE, AclPermissionType.ALLOW));

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @Mock
    AclService aclService;

    @Mock
    StreamService streamService;

    @Mock
    NamespaceRepository namespaceRepository;

    @Mock
    Admin adminClient;

    @Mock
    DescribeAclsResult describeAclsResult;

    @Mock
    CreateAclsResult createAclsResult;

    @InjectMocks
    AccessControlEntryAsyncExecutor aclAsyncExecutor;

    @Test
    void shouldConvertPublicAcl() {
        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("*")
                        .build())
                .build();

        AclBinding aclBinding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "ns1-", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        "User:*",
                        "*",
                        AclOperation.fromString(acl.getSpec().getPermission().toString()),
                        AclPermissionType.ALLOW));

        assertEquals(aclBinding, aclAsyncExecutor.convertPublicAcl(acl));
    }

    @Test
    void shouldMarkAclAsSuccessWhenCreated() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(true);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(any())).thenReturn(false);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(adminClient.createAcls(anyCollection())).thenAnswer(invocation -> {
            when(createAclsResult.values())
                    .thenReturn(invocation.<Collection<AclBinding>>getArgument(0).stream()
                            .collect(Collectors.toMap(
                                    Function.identity(), _ -> KafkaFuture.<Void>completedFuture(null))));
            return createAclsResult;
        });
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        aclAsyncExecutor.run();

        verify(adminClient).createAcls(argThat(acls -> acls.contains(READ_ACL_BINDING)));
        verify(aclService)
                .create(argThat(
                        a -> a == acl && a.isSuccess() && a.getMetadata().getGeneration() == 1));
    }

    @Test
    void shouldMarkAclAsFailedWhenCreationFails() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(true);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(any())).thenReturn(false);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(adminClient.createAcls(anyCollection())).thenAnswer(invocation -> {
            KafkaFutureImpl<Void> failedCreation = new KafkaFutureImpl<>();
            failedCreation.completeExceptionally(new RuntimeException("error"));
            when(createAclsResult.values())
                    .thenReturn(invocation.<Collection<AclBinding>>getArgument(0).stream()
                            .collect(Collectors.toMap(Function.identity(), _ -> failedCreation)));
            return createAclsResult;
        });
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        aclAsyncExecutor.run();

        verify(aclService)
                .create(argThat(a -> a == acl
                        && a.isFailed()
                        && !a.isCreated()
                        && a.getMetadata().getStatus().getMessage().contains("error")));
    }

    @ParameterizedTest
    @MethodSource("failedOrLegacyStatuses")
    void shouldMarkFailedOrLegacyAclAsSuccessWhenAlreadyOnBroker(Resource.Metadata.Status status) {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(status)
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(true);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of(READ_ACL_BINDING)));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(any())).thenReturn(false);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        aclAsyncExecutor.run();

        verify(adminClient, never()).createAcls(argThat(acls -> !acls.isEmpty()));
        verify(aclService)
                .create(argThat(
                        a -> a == acl && a.isSuccess() && a.getMetadata().getGeneration() == 1));
    }

    @Test
    void shouldNotPersistAclWhenSuccessAndAlreadyOnBroker() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofSuccess())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(true);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of(READ_ACL_BINDING)));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(any())).thenReturn(false);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));

        aclAsyncExecutor.run();

        verify(aclService, never()).findByName(any(), any());
        verify(aclService, never()).create(any());
    }

    @Test
    void shouldMarkKafkaStreamAsSuccessWhenAclsOnBroker() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(true);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values())
                .thenReturn(KafkaFuture.completedFuture(List.of(STREAM_CREATE_ACL_BINDING, STREAM_DELETE_ACL_BINDING)));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of());
        when(streamService.findAllForCluster("local")).thenReturn(List.of(kafkaStream));
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(kafkaStream));

        aclAsyncExecutor.run();

        verify(streamService).create(argThat(ks -> ks == kafkaStream && ks.isSuccess()));
    }

    @Test
    void shouldNotPersistAclDeletedDuringSynchronization() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(true);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(any())).thenReturn(false);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(adminClient.createAcls(anyCollection())).thenAnswer(invocation -> {
            when(createAclsResult.values())
                    .thenReturn(invocation.<Collection<AclBinding>>getArgument(0).stream()
                            .collect(Collectors.toMap(
                                    Function.identity(), _ -> KafkaFuture.<Void>completedFuture(null))));
            return createAclsResult;
        });
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.empty());

        aclAsyncExecutor.run();

        verify(aclService, never()).create(any());
    }

    @Test
    void shouldNotPersistAclReappliedDuringSynchronization() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();
        AccessControlEntry storedAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT.plusSeconds(1)))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(true);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(any())).thenReturn(false);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(adminClient.createAcls(anyCollection())).thenAnswer(invocation -> {
            when(createAclsResult.values())
                    .thenReturn(invocation.<Collection<AclBinding>>getArgument(0).stream()
                            .collect(Collectors.toMap(
                                    Function.identity(), _ -> KafkaFuture.<Void>completedFuture(null))));
            return createAclsResult;
        });
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(storedAcl));

        aclAsyncExecutor.run();

        verify(aclService, never()).create(any());
    }

    @Test
    void shouldNotUpdateNonPublicAclStatusWhenClusterDoesNotManageAcls() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(false);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of(READ_ACL_BINDING)));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(any())).thenReturn(false);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(managedClusterProperties.isManageRbac()).thenReturn(true);

        aclAsyncExecutor.run();

        verify(aclService, never()).findByName(any(), any());
        verify(aclService, never()).create(any());
    }

    @Test
    void shouldSkipAclsGrantedToDeletedNamespaces() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();
        AccessControlEntry orphanAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-orphan-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("deleted-namespace")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(true);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(acl, orphanAcl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(any())).thenReturn(false);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(adminClient.createAcls(anyCollection())).thenAnswer(invocation -> {
            when(createAclsResult.values())
                    .thenReturn(invocation.<Collection<AclBinding>>getArgument(0).stream()
                            .collect(Collectors.toMap(
                                    Function.identity(), _ -> KafkaFuture.<Void>completedFuture(null))));
            return createAclsResult;
        });
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        aclAsyncExecutor.run();

        verify(adminClient).createAcls(argThat(acls -> acls.size() == 1 && acls.contains(READ_ACL_BINDING)));
        verify(aclService, never()).findByName("ns1", "ns1-orphan-acl");
    }

    @Test
    void shouldCreatePublicAclWhenClusterOnlyManagesRbac() {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry publicAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .namespace("ns1")
                        .name("ns1-acl")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(INSTANT))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("*")
                        .build())
                .build();
        AclBinding publicAclBinding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "ns1-", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        "User:*", "*", AclOperation.READ, AclPermissionType.ALLOW));

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(false);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(List.of(publicAcl));
        when(streamService.findAllForCluster("local")).thenReturn(List.of());
        when(aclService.isPublicAcl(publicAcl)).thenReturn(true);
        when(adminClient.createAcls(anyCollection())).thenAnswer(invocation -> {
            when(createAclsResult.values())
                    .thenReturn(invocation.<Collection<AclBinding>>getArgument(0).stream()
                            .collect(Collectors.toMap(
                                    Function.identity(), _ -> KafkaFuture.<Void>completedFuture(null))));
            return createAclsResult;
        });
        when(managedClusterProperties.isManageRbac()).thenReturn(true);
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(publicAcl));

        aclAsyncExecutor.run();

        verify(adminClient).createAcls(List.of(publicAclBinding));
        verify(aclService).create(argThat(a -> a == publicAcl && a.isSuccess()));
    }

    static Stream<Resource.Metadata.Status> failedOrLegacyStatuses() {
        return Stream.of(Resource.Metadata.Status.ofFailed("error"), null);
    }
}
