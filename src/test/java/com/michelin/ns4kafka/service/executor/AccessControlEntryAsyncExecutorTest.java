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
import static org.mockito.Mockito.mock;
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
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.params.provider.CsvSource;
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
        AccessControlEntry acl = buildAcl(Resource.Metadata.Status.ofPending());

        stubSynchronization(true, List.of(), List.of(acl), List.of());
        stubAclCreation(Map.of());
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        aclAsyncExecutor.run();

        verify(adminClient).createAcls(argThat(acls -> acls.contains(READ_ACL_BINDING)));
        verify(aclService)
                .create(argThat(
                        a -> a == acl && a.isSuccess() && a.getMetadata().getGeneration() == 1));
    }

    @Test
    void shouldMarkAclAsFailedWhenCreationFails() {
        AccessControlEntry acl = buildAcl(Resource.Metadata.Status.ofPending());

        stubSynchronization(true, List.of(), List.of(acl), List.of());
        stubAclCreation(Map.of(READ_ACL_BINDING, new RuntimeException("error")));
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        aclAsyncExecutor.run();

        verify(aclService)
                .create(argThat(a -> a == acl
                        && a.isFailed()
                        && !a.isCreated()
                        && a.getMetadata().getStatus().getMessage().contains("error")));
    }

    @ParameterizedTest
    @MethodSource("unresolvedStatuses")
    void shouldMarkAclAsSuccessWhenAlreadyOnBroker(Resource.Metadata.Status status) {
        AccessControlEntry acl = buildAcl(status);

        stubSynchronization(true, List.of(READ_ACL_BINDING), List.of(acl), List.of());
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        aclAsyncExecutor.run();

        verify(adminClient, never()).createAcls(argThat(acls -> !acls.isEmpty()));
        verify(aclService)
                .create(argThat(
                        a -> a == acl && a.isSuccess() && a.getMetadata().getGeneration() == 1));
    }

    @Test
    void shouldNotPersistAclWhenSuccessAndAlreadyOnBroker() {
        AccessControlEntry acl = buildAcl(Resource.Metadata.Status.ofSuccess());

        stubSynchronization(true, List.of(READ_ACL_BINDING), List.of(acl), List.of());

        aclAsyncExecutor.run();

        verify(aclService, never()).findByName(any(), any());
        verify(aclService, never()).create(any());
    }

    @Test
    void shouldMarkKafkaStreamAsSuccessWhenAclsOnBroker() {
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

        stubSynchronization(
                true, List.of(STREAM_CREATE_ACL_BINDING, STREAM_DELETE_ACL_BINDING), List.of(), List.of(kafkaStream));
        when(streamService.findByName(buildNamespace(), "ns1-stream")).thenReturn(Optional.of(kafkaStream));

        aclAsyncExecutor.run();

        verify(streamService).create(argThat(ks -> ks == kafkaStream && ks.isSuccess()));
    }

    @ParameterizedTest
    @CsvSource({"deleted, false", "reapplied, false", "nullStoredTimestamp, true", "nullReadTimestamp, false"})
    void shouldPersistAclOnlyWhenUnchangedSinceLastApply(String scenario, boolean shouldPersist) {
        AccessControlEntry acl = buildAcl(Resource.Metadata.Status.ofPending());
        AccessControlEntry storedAcl = buildAcl(Resource.Metadata.Status.ofPending());

        switch (scenario) {
            case "reapplied" -> storedAcl.getMetadata().setUpdateTimestamp(Date.from(INSTANT.plusSeconds(1)));
            case "nullStoredTimestamp" -> storedAcl.getMetadata().setUpdateTimestamp(null);
            case "nullReadTimestamp" -> acl.getMetadata().setUpdateTimestamp(null);
            default -> {
                // Deleted
            }
        }

        stubSynchronization(true, List.of(), List.of(acl), List.of());
        stubAclCreation(Map.of());
        when(aclService.findByName("ns1", "ns1-acl"))
                .thenReturn("deleted".equals(scenario) ? Optional.empty() : Optional.of(storedAcl));

        aclAsyncExecutor.run();

        verify(aclService, times(shouldPersist ? 1 : 0)).create(argThat(a -> a == acl && a.isSuccess()));
    }

    @Test
    void shouldNotUpdateNonPublicAclStatusWhenClusterDoesNotManageAcls() {
        AccessControlEntry acl = buildAcl(Resource.Metadata.Status.ofPending());

        stubSynchronization(false, List.of(READ_ACL_BINDING), List.of(acl), List.of());
        when(managedClusterProperties.isManageRbac()).thenReturn(true);

        aclAsyncExecutor.run();

        verify(aclService, never()).findByName(any(), any());
        verify(aclService, never()).create(any());
    }

    @Test
    void shouldSkipAclsGrantedToDeletedNamespaces() {
        AccessControlEntry acl = buildAcl(Resource.Metadata.Status.ofPending());
        AccessControlEntry orphanAcl = buildAcl(Resource.Metadata.Status.ofPending());
        orphanAcl.getMetadata().setName("ns1-orphan-acl");
        orphanAcl.getSpec().setGrantedTo("deleted-namespace");

        stubSynchronization(true, List.of(), List.of(acl, orphanAcl), List.of());
        stubAclCreation(Map.of());
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        aclAsyncExecutor.run();

        verify(adminClient).createAcls(argThat(acls -> acls.size() == 1 && acls.contains(READ_ACL_BINDING)));
        verify(aclService).create(argThat(a -> a == acl && a.isSuccess()));
        verify(aclService, never()).findByName("ns1", "ns1-orphan-acl");
    }

    static Stream<Resource.Metadata.Status> unresolvedStatuses() {
        return Stream.of(Resource.Metadata.Status.ofPending(), Resource.Metadata.Status.ofFailed("error"), null);
    }

    private static Namespace buildNamespace() {
        return Namespace.builder()
                .metadata(
                        Resource.Metadata.builder().name("ns1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();
    }

    private static AccessControlEntry buildAcl(Resource.Metadata.Status status) {
        return AccessControlEntry.builder()
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
    }

    private void stubSynchronization(
            boolean manageAcls,
            List<AclBinding> brokerAcls,
            List<AccessControlEntry> acls,
            List<KafkaStream> kafkaStreams) {
        Namespace namespace = buildNamespace();

        when(managedClusterProperties.getName()).thenReturn("local");
        when(managedClusterProperties.isManageAcls()).thenReturn(manageAcls);
        when(managedClusterProperties.getTimeout()).thenReturn(new ManagedClusterProperties.TimeoutProperties());
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture((Collection<AclBinding>) brokerAcls));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(acls);
        when(streamService.findAllForCluster("local")).thenReturn(kafkaStreams);

        if (!acls.isEmpty()) {
            when(aclService.isPublicAcl(any())).thenReturn(false);
        }

        if (!acls.isEmpty() || !kafkaStreams.isEmpty()) {
            when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        }
    }

    private void stubAclCreation(Map<AclBinding, Exception> creationErrors) {
        when(adminClient.createAcls(anyCollection())).thenAnswer(invocation -> {
            Collection<AclBinding> toCreate = invocation.getArgument(0);
            Map<AclBinding, KafkaFuture<Void>> results = toCreate.stream()
                    .collect(Collectors.toMap(Function.identity(), aclBinding -> {
                        if (creationErrors.containsKey(aclBinding)) {
                            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                            future.completeExceptionally(creationErrors.get(aclBinding));
                            return future;
                        }
                        return KafkaFuture.completedFuture(null);
                    }));
            CreateAclsResult result = mock(CreateAclsResult.class);
            when(result.values()).thenReturn(results);
            return result;
        });
    }
}
