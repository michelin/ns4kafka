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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.repository.kafka.KafkaStreamRepository;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingRequest;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingResponse;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ConfluentRoleBindingAsyncExecutorTest {

    @Mock
    ConfluentCloudClient confluentCloudClient;

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @InjectMocks
    ConfluentRoleBindingAsyncExecutor rbAsyncExecutor;

    @Mock
    NamespaceRepository namespaceRepository;

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

        when(namespaceRepository.findByName("ns1"))
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

        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(ns));

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

        when(namespaceRepository.findByName("ns1"))
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

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        assertEquals(readGroupRoleBinding, rbAsyncExecutor.convertKafkaStreamToRoleBinding(kafkaStream));
    }

    @Test
    void shouldCreateAcl() {
        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
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

        AccessControlEntry successAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofSuccess())
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

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.empty());
        when(aclRepository.create(successAcl)).thenReturn(successAcl);

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository).create(successAcl);
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldNotCreateAclWhenChangedSinceLastApply() {
        Instant instant = Instant.now();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant))
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
                        .creationTimestamp(Date.from(instant.plus(1, ChronoUnit.SECONDS)))
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

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.of(newAcl));

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository, never()).create(any());
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateAclWhenErrorCreating() {
        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
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

        AccessControlEntry failedAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofFailed("error"))
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
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
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.empty());
        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(aclRepository.create(acl)).thenReturn(failedAcl);

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository).create(failedAcl);
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateAclWhenErrorCreatingAndChangedSinceLastApply() {
        Instant instant = Instant.now();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant))
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
                        .creationTimestamp(Date.from(instant.plus(1, ChronoUnit.SECONDS)))
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
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
        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.of(newAcl));

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository, never()).create(any());
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldDeleteAcls() {
        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-read")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofDeleting())
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
                        .status(Resource.Metadata.Status.ofDeleting())
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
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", readRoleBinding)).thenReturn(Mono.just(response));
        when(confluentCloudClient.deleteRoleBinding("cluster", readEmptyRoleBinding))
                .thenReturn(Mono.empty());
        when(aclService.findByName("ns1", "ns1-read")).thenReturn(Optional.empty());
        doNothing().when(aclRepository).delete(acl);
        doNothing().when(aclRepository).delete(emptyResponseAcl);

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl, emptyResponseAcl));

        verify(aclRepository, never()).create(any());
        verify(aclRepository, times(2)).delete(argThat(ks -> ks.equals(emptyResponseAcl) || ks.equals(acl)));
    }

    @Test
    void shouldNotDeleteAclWhenChangedSinceLastApply() {
        Instant instant = Instant.now();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-read")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .creationTimestamp(Date.from(instant))
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry newAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-read")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant.plus(1, ChronoUnit.SECONDS)))
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.TOPIC, "ns1-*");
        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", readRoleBinding)).thenReturn(Mono.just(response));
        when(aclService.findByName("ns1", "ns1-read")).thenReturn(Optional.of(newAcl));

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository, never()).create(any());
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateAclWhenErrorDeleting() {
        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry failedAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-write")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofFailed("error"))
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
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", writeRoleBinding))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(aclService.findByName("ns1", "ns1-write")).thenReturn(Optional.empty());
        when(aclRepository.create(failedAcl)).thenReturn(failedAcl);

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository).create(failedAcl);
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateAclWhenErrorDeletingAndChangedSinceLastApply() {
        Instant instant = Instant.now();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-read")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .creationTimestamp(Date.from(instant))
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry newAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-read")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant.plus(1, ChronoUnit.SECONDS)))
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.TOPIC, "ns1-*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", readRoleBinding))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(aclService.findByName("ns1", "ns1-read")).thenReturn(Optional.of(newAcl));

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl));

        verify(aclRepository, never()).create(any());
        verify(aclRepository, never()).delete(any());
    }

    @Test
    void shouldCreateKafkaStream() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .build();

        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.empty());
        when(kafkaStreamRepository.create(kafkaStream)).thenReturn(kafkaStream);

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository).create(kafkaStream);
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldNotCreateKafkaStreamWhenChangedSinceLastApply() {
        Instant instant = Instant.now();

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant))
                        .build())
                .build();

        KafkaStream newKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant.plus(1, ChronoUnit.SECONDS)))
                        .build())
                .build();

        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(newKafkaStream));

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository, never()).create(any());
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateKafkaStreamsWhenErrorCreating() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .build();

        KafkaStream failedKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofFailed("error"))
                        .build())
                .build();

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.empty());
        when(kafkaStreamRepository.create(failedKafkaStream)).thenReturn(failedKafkaStream);

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository).create(failedKafkaStream);
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateKafkaStreamsWhenErrorCreatingAndChangedSinceLastApply() {
        Instant instant = Instant.now();

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant))
                        .build())
                .build();

        KafkaStream newKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant.plus(1, ChronoUnit.SECONDS)))
                        .build())
                .build();

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
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
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .build())
                .build();

        KafkaStream emptyResponseKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream-empty")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .build())
                .build();

        RoleBinding manageTopicRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");
        RoleBinding manageTopicRoleBindingEmpty = new RoleBinding(
                "User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream-empty*");
        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", manageTopicRoleBinding))
                .thenReturn(Mono.just(response));
        when(confluentCloudClient.deleteRoleBinding("cluster", manageTopicRoleBindingEmpty))
                .thenReturn(Mono.empty());
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.empty());
        doNothing().when(kafkaStreamRepository).delete(kafkaStream);
        doNothing().when(kafkaStreamRepository).delete(emptyResponseKafkaStream);

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream, emptyResponseKafkaStream));

        verify(kafkaStreamRepository, never()).create(any());
        verify(kafkaStreamRepository, times(2))
                .delete(argThat(ks -> ks.equals(emptyResponseKafkaStream) || ks.equals(kafkaStream)));
    }

    @Test
    void shouldNotDeleteKafkaStreamWhenChangedSinceLastUpdate() {
        Instant instant = Instant.now();
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .creationTimestamp(Date.from(instant))
                        .build())
                .build();

        KafkaStream newKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant.plus(1, ChronoUnit.SECONDS)))
                        .build())
                .build();

        RoleBinding manageTopicRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");
        RoleBindingResponse response = RoleBindingResponse.builder().build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", manageTopicRoleBinding))
                .thenReturn(Mono.just(response));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(newKafkaStream));

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository, never()).create(any());
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldUpdateKafkaStreamsWhenErrorDeleting() {
        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .build())
                .build();

        KafkaStream failedKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofFailed("error"))
                        .build())
                .build();

        RoleBinding manageTopicRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", manageTopicRoleBinding))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(kafkaStreamRepository.create(failedKafkaStream)).thenReturn(failedKafkaStream);

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(kafkaStreamRepository).create(failedKafkaStream);
        verify(kafkaStreamRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateKafkaStreamsWhenErrorDeletingAndChangedSinceLastApply() {
        Instant instant = Instant.now();
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns1").build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .creationTimestamp(Date.from(instant))
                        .build())
                .build();

        KafkaStream newKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .creationTimestamp(Date.from(instant.plus(1, ChronoUnit.SECONDS)))
                        .build())
                .build();

        RoleBinding manageTopicRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", manageTopicRoleBinding))
                .thenReturn(Mono.error(new RuntimeException("error")));
        when(namespaceService.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(streamService.findByName(namespace, "ns1-stream")).thenReturn(Optional.of(newKafkaStream));

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
        when(aclService.findNonPublicToDeployForCluster("cluster")).thenReturn(List.of(acl));
        when(streamService.findAllToDeployForCluster("cluster")).thenReturn(List.of());
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        when(confluentCloudClient.createRoleBinding(any(), any())).thenReturn(Mono.just(response));
        when(aclRepository.create(acl)).thenReturn(acl);

        rbAsyncExecutor.synchronizeConfluentRoleBindings();

        verify(confluentCloudClient).createRoleBinding(any(), any());
        verify(aclRepository).create(acl);
    }

    @Test
    void shouldNotSynchronizeRoleBindings() {
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

        when(managedClusterProperties.getName()).thenReturn("cluster");
        doThrow(new KafkaStoreException("exception")).when(aclService).findNonPublicToDeployForCluster("cluster");

        rbAsyncExecutor.synchronizeConfluentRoleBindings();

        verify(aclService, never()).create(acl);
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
