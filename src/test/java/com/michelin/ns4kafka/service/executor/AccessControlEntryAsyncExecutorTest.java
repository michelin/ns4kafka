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
import static org.mockito.ArgumentMatchers.anyList;
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
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AccessControlEntryAsyncExecutorTest {
    private static final Instant instant = Instant.parse("2026-01-01T00:00:00Z");

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @InjectMocks
    AccessControlEntryAsyncExecutor aclAsyncExecutor;

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

    @Mock
    Admin adminClient;

    @Mock
    CreateAclsResult createAclsResult;

    @Mock
    KafkaFuture<Void> kafkaFuture;

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

        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));

        List<AclBinding> aclBindings = aclAsyncExecutor.convertAclToAclBindings(acl);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.createAcls(anyList())).thenReturn(createAclsResult);
        when(createAclsResult.values()).thenReturn(Map.of(aclBindings.getFirst(), kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.AclProperties aclProperties =
                new ManagedClusterProperties.TimeoutProperties.AclProperties();
        aclProperties.setCreate(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setAcl(aclProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.of(newAcl));
        when(aclRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        aclAsyncExecutor.createAcls(List.of(acl));

        verify(aclRepository).create(argThat(a -> a.equals(newAcl) && a.isPending() && a.isCreated()));
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateAclWhenErrorCreating() throws ExecutionException, InterruptedException, TimeoutException {
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

        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));

        List<AclBinding> aclBindings = aclAsyncExecutor.convertAclToAclBindings(acl);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.createAcls(anyList())).thenReturn(createAclsResult);
        when(createAclsResult.values()).thenReturn(Map.of(aclBindings.getFirst(), kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.AclProperties aclProperties =
                new ManagedClusterProperties.TimeoutProperties.AclProperties();
        aclProperties.setCreate(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setAcl(aclProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("error", new Throwable()));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.empty());
        when(aclRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        aclAsyncExecutor.createAcls(List.of(acl));

        verify(aclRepository)
                .create(argThat(a -> a.isFailed()
                        && !a.isCreated()
                        && "error".equals(a.getMetadata().getStatus().getMessage())));
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateAclWhenErrorCreatingAndChangedSinceLastApply()
            throws ExecutionException, InterruptedException, TimeoutException {
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

        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));

        List<AclBinding> aclBindings = aclAsyncExecutor.convertAclToAclBindings(acl);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.createAcls(anyList())).thenReturn(createAclsResult);
        when(createAclsResult.values()).thenReturn(Map.of(aclBindings.getFirst(), kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.AclProperties aclProperties =
                new ManagedClusterProperties.TimeoutProperties.AclProperties();
        aclProperties.setCreate(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setAcl(aclProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("error", new Throwable()));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.of(newAcl));

        aclAsyncExecutor.createAcls(List.of(acl));

        verify(aclRepository, never()).create(any());
        verify(aclRepository, never()).delete(any());
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

        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));

        List<AclBinding> aclBindings = aclAsyncExecutor.convertKafkaStreamToAclBindings(kafkaStream);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.createAcls(anyList())).thenReturn(createAclsResult);
        when(createAclsResult.values()).thenReturn(Map.of(aclBindings.getFirst(), kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.AclProperties aclProperties =
                new ManagedClusterProperties.TimeoutProperties.AclProperties();
        aclProperties.setCreate(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setAcl(aclProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(newKafkaStream));
        when(kafkaStreamRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        aclAsyncExecutor.createAclsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository).create(argThat(a -> a.equals(newKafkaStream) && a.isPending() && a.isCreated()));
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateKafkaStreamsWhenErrorCreating() throws ExecutionException, InterruptedException, TimeoutException {
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

        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));

        List<AclBinding> aclBindings = aclAsyncExecutor.convertKafkaStreamToAclBindings(kafkaStream);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.createAcls(anyList())).thenReturn(createAclsResult);
        when(createAclsResult.values()).thenReturn(Map.of(aclBindings.getFirst(), kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.AclProperties aclProperties =
                new ManagedClusterProperties.TimeoutProperties.AclProperties();
        aclProperties.setCreate(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setAcl(aclProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("error", new Throwable()));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.empty());
        when(kafkaStreamRepository.create(any())).thenAnswer(invocation -> invocation.getArgument(0));

        aclAsyncExecutor.createAclsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository)
                .create(argThat(a -> a.isFailed()
                        && !a.isCreated()
                        && "error".equals(a.getMetadata().getStatus().getMessage())));
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateKafkaStreamsWhenErrorCreatingAndChangedSinceLastApply()
            throws ExecutionException, InterruptedException, TimeoutException {
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

        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));

        List<AclBinding> aclBindings = aclAsyncExecutor.convertKafkaStreamToAclBindings(kafkaStream);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.createAcls(anyList())).thenReturn(createAclsResult);
        when(createAclsResult.values()).thenReturn(Map.of(aclBindings.getFirst(), kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.AclProperties aclProperties =
                new ManagedClusterProperties.TimeoutProperties.AclProperties();
        aclProperties.setCreate(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setAcl(aclProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("error", new Throwable()));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(newKafkaStream));

        aclAsyncExecutor.createAclsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository, never()).create(any());
        verify(kafkaStreamRepository, never()).delete(any());
    }
}
