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

import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_MANAGE;
import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_READ;
import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStreamRepository;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfluentRoleBindingAsyncExecutorTest {

    @Mock
    ConfluentCloudClient confluentCloudClient;

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @Mock
    Ns4KafkaProperties ns4KafkaProperties;

    @InjectMocks
    ConfluentRoleBindingAsyncExecutor rbAsyncExecutor;

    @Mock
    NamespaceRepository namespaceRepository;

    @Mock
    KafkaStreamRepository kafkaStreamRepository;

    @Mock
    AccessControlEntryRepository aclRepository;

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

        RoleBinding readGroupRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.GROUP, "connect-ns1-*");

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
                        .resourceType(AccessControlEntry.ResourceType.GROUP)
                        .resource("ns1-group")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readGroupRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.GROUP, "ns1-group");

        Namespace ns = Namespace.builder()
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(ns));
        when(streamService.hasKafkaStream(ns)).thenReturn(false);

        List<RoleBinding> ownerRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(ownerAcl);

        assertEquals(1, ownerRoleBindings.size());
        assertTrue(ownerRoleBindings.contains(readGroupRoleBinding));
    }

    @Test
    void shouldConvertGroupAclToRoleBindingWhenTransactionEnabled() {
        AccessControlEntry ownerAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.GROUP)
                        .resource("ns1-group")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readGroupRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.GROUP, "ns1-group");

        RoleBinding writeTransRoleBinding = new RoleBinding(
                "User:user1", DEVELOPER_WRITE, AccessControlEntry.ResourceType.TRANSACTIONAL_ID, "ns1-group*");

        RoleBinding writeTransRoleBinding2 = new RoleBinding(
                "User:user1",
                DEVELOPER_WRITE,
                AccessControlEntry.ResourceType.TRANSACTIONAL_ID,
                "connect-cluster-ns1-group*");

        Namespace ns = Namespace.builder()
                .spec(Namespace.NamespaceSpec.builder()
                        .transactionsEnabled(true)
                        .kafkaUser("user1")
                        .build())
                .build();

        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(ns));

        List<RoleBinding> ownerRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(ownerAcl);

        assertEquals(3, ownerRoleBindings.size());
        assertTrue(ownerRoleBindings.containsAll(
                List.of(readGroupRoleBinding, writeTransRoleBinding, writeTransRoleBinding2)));
    }

    @Test
    void shouldConvertGroupAclToRoleBindingWhenHasKafkaStream() {
        AccessControlEntry ownerAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.GROUP)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        RoleBinding readGroupRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.GROUP, "ns1-*");

        RoleBinding writeTransRoleBinding = new RoleBinding(
                "User:user1", DEVELOPER_WRITE, AccessControlEntry.ResourceType.TRANSACTIONAL_ID, "ns1-*");

        Namespace ns = Namespace.builder()
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(ns));
        when(streamService.hasKafkaStream(ns)).thenReturn(true);

        List<RoleBinding> ownerRoleBindings = rbAsyncExecutor.convertAclToRoleBinding(ownerAcl);

        assertEquals(2, ownerRoleBindings.size());
        assertTrue(ownerRoleBindings.containsAll(List.of(readGroupRoleBinding, writeTransRoleBinding)));
    }

    @Test
    void shouldNotConvertWrongGroupAclToRoleBinding() {
        AccessControlEntry writeAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.GROUP)
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
    void shouldCreateRoleBindingsFromAcls() {
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

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        doNothing().when(confluentCloudClient).createRoleBinding(any(), any());
        when(aclRepository.create(successAcl)).thenReturn(successAcl);

        rbAsyncExecutor.createRoleBindingsFromAcls(List.of(acl));

        verify(confluentCloudClient).createRoleBinding(any(), any());
        verify(aclRepository).create(successAcl);
    }

    @Test
    void shouldDeleteRoleBindingsFromAcls() {
        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-read")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();
        RoleBinding readRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_READ, AccessControlEntry.ResourceType.TOPIC, "ns1-*");

        when(managedClusterProperties.isManageRbac()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        doNothing().when(confluentCloudClient).deleteRoleBinding("cluster", readRoleBinding);

        rbAsyncExecutor.deleteRoleBindingsFromACL(acl);

        verify(confluentCloudClient).deleteRoleBinding("cluster", readRoleBinding);
    }

    @Test
    void shouldCreateKafkaStreamRoleBinding() {
        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .build())
                .build();

        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));

        doNothing().when(confluentCloudClient).createRoleBinding(any(), any());
        when(kafkaStreamRepository.create(kafkaStream)).thenReturn(kafkaStream);

        rbAsyncExecutor.createRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(confluentCloudClient).createRoleBinding(any(), any());
        verify(kafkaStreamRepository).create(kafkaStream);
    }

    @Test
    void shouldDeleteKafkaStreamRoleBinding() {
        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .build())
                .build();
        RoleBinding manageTopicRoleBinding =
                new RoleBinding("User:user1", DEVELOPER_MANAGE, AccessControlEntry.ResourceType.TOPIC, "ns1-stream*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.isManageRbac()).thenReturn(true);
        when(namespaceRepository.findByName("ns1"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user1")
                                .build())
                        .build()));
        doNothing().when(confluentCloudClient).deleteRoleBinding("cluster", manageTopicRoleBinding);

        rbAsyncExecutor.deleteRoleBindingFromKafkaStream(kafkaStream);

        verify(confluentCloudClient).deleteRoleBinding("cluster", manageTopicRoleBinding);
    }
}
