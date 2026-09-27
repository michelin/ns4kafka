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
    void shouldDeleteAcls() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

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
        RoleBindingRequest readRequest = new RoleBindingRequest(readRoleBinding, confluentCloudProperties);
        RoleBindingRequest readEmptyRequest = new RoleBindingRequest(readEmptyRoleBinding, confluentCloudProperties);

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.listRoleBindings("cluster", readRequest.crnPattern()))
                .thenReturn(Flux.just(
                        // Same resource, but other principal or other role
                        RoleBindingResponse.builder()
                                .id("rb-other-user")
                                .principal("User:user2")
                                .roleName("DeveloperRead")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-*")
                                .build(),
                        RoleBindingResponse.builder()
                                .id("rb-other-role")
                                .principal("User:user1")
                                .roleName("DeveloperWrite")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-*")
                                .build(),
                        RoleBindingResponse.builder()
                                .id("rb-match")
                                .principal("User:user1")
                                .roleName("DeveloperRead")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-*")
                                .build()));
        when(confluentCloudClient.listRoleBindings("cluster", readEmptyRequest.crnPattern()))
                .thenReturn(Flux.empty());
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-match"))
                .thenReturn(Mono.just(RoleBindingResponse.builder()
                        .id("rb-match")
                        .principal("User:user1")
                        .roleName("DeveloperRead")
                        .crnPattern(
                                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-*")
                        .build()));

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl, emptyResponseAcl));

        verify(confluentCloudClient).deleteRoleBinding("cluster", "rb-match");
        verify(confluentCloudClient, times(1)).deleteRoleBinding(any(), anyString());
        verify(aclService, never()).create(any());
    }

    @Test
    void shouldNotFailWhenErrorDeletingAcl() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

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
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", new RoleBindingRequest(writeRoleBinding, confluentCloudProperties).crnPattern()))
                .thenReturn(Flux.just(RoleBindingResponse.builder()
                        .id("rb-write")
                        .principal("User:user1")
                        .roleName("DeveloperWrite")
                        .crnPattern(
                                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-*")
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-write"))
                .thenReturn(Mono.error(new RuntimeException("error")));

        rbAsyncExecutor.deleteRoleBindingsFromAcls(List.of(acl));

        verify(aclService, never()).create(any());
    }

    @Test
    void shouldDeleteKafkaStreams() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

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
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.listRoleBindings(
                        "cluster",
                        new RoleBindingRequest(manageTopicRoleBinding, confluentCloudProperties).crnPattern()))
                .thenReturn(Flux.just(RoleBindingResponse.builder()
                        .id("rb-manage")
                        .principal("User:user1")
                        .roleName("DeveloperManage")
                        .crnPattern(
                                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-stream*")
                        .build()));
        when(confluentCloudClient.listRoleBindings(
                        "cluster",
                        new RoleBindingRequest(manageTopicRoleBindingEmpty, confluentCloudProperties).crnPattern()))
                .thenReturn(Flux.empty());
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-manage"))
                .thenReturn(Mono.just(RoleBindingResponse.builder()
                        .id("rb-manage")
                        .principal("User:user1")
                        .roleName("DeveloperManage")
                        .crnPattern(
                                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-stream*")
                        .build()));

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream, emptyResponseKafkaStream));

        verify(confluentCloudClient).deleteRoleBinding("cluster", "rb-manage");
        verify(confluentCloudClient, times(1)).deleteRoleBinding(any(), anyString());
        verify(streamService, never()).create(any());
    }

    @Test
    void shouldNotFailWhenErrorDeletingKafkaStream() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

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
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(confluentCloudClient.listRoleBindings(
                        "cluster",
                        new RoleBindingRequest(manageTopicRoleBinding, confluentCloudProperties).crnPattern()))
                .thenReturn(Flux.just(RoleBindingResponse.builder()
                        .id("rb-manage")
                        .principal("User:user1")
                        .roleName("DeveloperManage")
                        .crnPattern(
                                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-stream*")
                        .build()));
        when(confluentCloudClient.deleteRoleBinding("cluster", "rb-manage"))
                .thenReturn(Mono.error(new RuntimeException("error")));

        rbAsyncExecutor.deleteRoleBindingsFromKafkaStreams(List.of(kafkaStream));

        verify(streamService, never()).create(any());
    }

    @Test
    void shouldCreateMissingAndDeleteUnsynchronizedRoleBindings() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();
        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofSuccess())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .build();
        RoleBinding writeRoleBinding = new RoleBinding("User:user1", DEVELOPER_WRITE, TOPIC, "ns1-*");
        RoleBindingResponse unsyncRoleBinding = RoleBindingResponse.builder()
                .id("rb-unsync")
                .principal("User:user1")
                .roleName("DeveloperRead")
                .crnPattern(
                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-old*")
                .build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(true);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.just(
                        RoleBindingResponse.builder()
                                .id("rb-read")
                                .principal("User:user1")
                                .roleName("DeveloperRead")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-*")
                                .build(),
                        RoleBindingResponse.builder()
                                .id("rb-manage")
                                .principal("User:user1")
                                .roleName("DeveloperManage")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-stream*")
                                .build(),
                        unsyncRoleBinding,
                        // Not managed by Ns4Kafka: other role or other principal
                        RoleBindingResponse.builder()
                                .id("rb-owner")
                                .principal("User:user1")
                                .roleName("ResourceOwner")
                                .crnPattern(RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "topic=*")
                                .build(),
                        RoleBindingResponse.builder()
                                .id("rb-other")
                                .principal("User:other")
                                .roleName("DeveloperRead")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=other-*")
                                .build()));
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("cluster")).thenReturn(List.of(kafkaStream));
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
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
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();
        RoleBinding readRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*");
        RoleBinding writeRoleBinding = new RoleBinding("User:user1", DEVELOPER_WRITE, TOPIC, "ns1-*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(true);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.just(
                        RoleBindingResponse.builder()
                                .id("rb-unsync-1")
                                .principal("User:user1")
                                .roleName("DeveloperRead")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=old1-*")
                                .build(),
                        RoleBindingResponse.builder()
                                .id("rb-unsync-2")
                                .principal("User:user1")
                                .roleName("DeveloperRead")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=old2-*")
                                .build()));
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("cluster")).thenReturn(List.of());
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
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
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(false);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.just(RoleBindingResponse.builder()
                        .id("rb-unsync")
                        .principal("User:user1")
                        .roleName("DeveloperRead")
                        .crnPattern(
                                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-old*")
                        .build()));
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(List.of());
        when(streamService.findAllForCluster("cluster")).thenReturn(List.of());
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, never()).deleteRoleBinding(any(), anyString());
        verify(confluentCloudClient, never()).createRoleBinding(any(), any());
    }

    @ParameterizedTest
    @MethodSource("failedOrLegacyStatuses")
    void shouldMarkFailedOrLegacyResourceAsSuccessWhenRoleBindingsAlreadyExist(Resource.Metadata.Status status) {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl")
                        .namespace("ns1")
                        .status(status)
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();
        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("ns1")
                        .name("ns1-stream")
                        .status(status)
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .build();
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(true);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.just(
                        RoleBindingResponse.builder()
                                .id("rb-read")
                                .principal("User:user1")
                                .roleName("DeveloperRead")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-*")
                                .build(),
                        RoleBindingResponse.builder()
                                .id("rb-manage")
                                .principal("User:user1")
                                .roleName("DeveloperManage")
                                .crnPattern(
                                        "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-stream*")
                                .build()));
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("cluster")).thenReturn(List.of(kafkaStream));
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
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
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl1 = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();
        AccessControlEntry acl2 = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl-2")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();
        RoleBinding readRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(true);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.empty());
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(List.of(acl1, acl2));
        when(streamService.findAllForCluster("cluster")).thenReturn(List.of());
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
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
    void shouldSkipResourcesOfDeletedNamespaces() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();
        AccessControlEntry orphanAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-orphan-acl")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("deleted-namespace")
                        .build())
                .build();
        KafkaStream orphanKafkaStream = KafkaStream.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .namespace("deleted-namespace")
                        .name("ns1-stream")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .build();
        RoleBinding readRoleBinding = new RoleBinding("User:user1", DEVELOPER_READ, TOPIC, "ns1-*");

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(true);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.empty());
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(List.of(acl, orphanAcl));
        when(streamService.findAllForCluster("cluster")).thenReturn(List.of(orphanKafkaStream));
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.createRoleBinding("cluster", readRoleBinding))
                .thenReturn(
                        Mono.just(RoleBindingResponse.builder().id("rb-read").build()));
        when(aclService.findByName("ns1", "ns1-acl")).thenReturn(Optional.of(acl));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, times(1)).createRoleBinding(any(), any());
        verify(aclService, never()).findByName("ns1", "ns1-orphan-acl");
        verify(streamService, never()).findByName(any(), any());
    }

    @Test
    void shouldNotPersistWhenSuccessAndRoleBindingsAlreadyExist() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofSuccess())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("ns1")
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(true);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.just(RoleBindingResponse.builder()
                        .id("rb-read")
                        .principal("User:user1")
                        .roleName("DeveloperRead")
                        .crnPattern(
                                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/topic=ns1-*")
                        .build()));
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("cluster")).thenReturn(List.of());
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, never()).createRoleBinding(any(), any());
        verify(aclService, never()).findByName(any(), any());
        verify(aclService, never()).create(any());
    }

    @ParameterizedTest
    @CsvSource({"deleted, false", "reapplied, false", "nullStoredTimestamp, true", "nullReadTimestamp, false"})
    void shouldPersistOnlyWhenUnchangedSinceLastApply(String scenario, boolean shouldPersist) {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();
        AccessControlEntry storedAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cluster")
                        .name("ns1-acl")
                        .namespace("ns1")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("ns1")
                        .build())
                .build();

        switch (scenario) {
            case "reapplied" -> storedAcl.getMetadata().setUpdateTimestamp(Date.from(instant.plusSeconds(1)));
            case "nullStoredTimestamp" -> storedAcl.getMetadata().setUpdateTimestamp(null);
            case "nullReadTimestamp" -> acl.getMetadata().setUpdateTimestamp(null);
            default -> {
                // Deleted
            }
        }

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isDropUnsyncAcls()).thenReturn(true);
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.empty());
        when(aclService.findAllNonPublicForCluster("cluster")).thenReturn(List.of(acl));
        when(streamService.findAllForCluster("cluster")).thenReturn(List.of());
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(namespaceRepository.findByName("ns1")).thenReturn(Optional.of(namespace));
        when(confluentCloudClient.createRoleBinding(any(), any()))
                .thenReturn(Mono.just(RoleBindingResponse.builder().build()));
        when(aclService.findByName("ns1", "ns1-acl"))
                .thenReturn("deleted".equals(scenario) ? Optional.empty() : Optional.of(storedAcl));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(aclService, times(shouldPersist ? 1 : 0)).create(argThat(a -> a == acl && a.isSuccess()));
    }

    @Test
    void shouldNotSynchronizeRoleBindingsWhenListingFails() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.error(new RuntimeException("error")));

        rbAsyncExecutor.synchronizeRoleBindings().block();

        verify(confluentCloudClient, never()).createRoleBinding(any(), any());
        verify(confluentCloudClient, never()).deleteRoleBinding(any(), anyString());
        verify(aclService, never()).create(any());
        verify(streamService, never()).create(any());
    }

    @Test
    void shouldSynchronizeRoleBindingsWhenClusterManagesRbac() {
        ManagedClusterProperties.ConfluentCloudProperties confluentCloudProperties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        confluentCloudProperties.setOrganizationId("orgId");
        confluentCloudProperties.setEnvironmentId("envId");
        confluentCloudProperties.setClusterId("clusterId");

        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user1").build())
                .build();

        when(managedClusterProperties.getName()).thenReturn("cluster");
        when(managedClusterProperties.getConfluentCloud()).thenReturn(confluentCloudProperties);
        when(managedClusterProperties.isManageAcls()).thenReturn(false);
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
        when(managedClusterProperties.isManageRbac()).thenReturn(true);
        when(namespaceRepository.findAllForCluster("cluster")).thenReturn(List.of(namespace));
        when(confluentCloudClient.listRoleBindings(
                        "cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*"))
                .thenReturn(Flux.empty());

        rbAsyncExecutor.run().block();

        verify(confluentCloudClient)
                .listRoleBindings("cluster", RoleBindingRequest.clusterCrnPattern(confluentCloudProperties) + "*");
    }

    @Test
    void shouldNotSynchronizeRoleBindingsWhenClusterManagesAcls() {
        when(managedClusterProperties.isManageAcls()).thenReturn(true);

        rbAsyncExecutor.run().block();

        verify(confluentCloudClient, never()).listRoleBindings(any(), any());
    }

    @Test
    void shouldCreateGroupAndTransactionalIdCrnPatterns() {
        ManagedClusterProperties.ConfluentCloudProperties properties =
                new ManagedClusterProperties.ConfluentCloudProperties();
        properties.setOrganizationId("orgId");
        properties.setEnvironmentId("envId");
        properties.setClusterId("clusterId");

        RoleBinding groupRoleBinding = new RoleBinding("User:user", DEVELOPER_READ, GROUP, "myGroup");
        RoleBindingRequest groupRbRequest = new RoleBindingRequest(groupRoleBinding, properties);
        String groupCrnPattern =
                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/group=myGroup";

        RoleBinding transIdRoleBinding = new RoleBinding("User:user", DEVELOPER_READ, TRANSACTIONAL_ID, "myTransId");
        RoleBindingRequest transIdRbRequest = new RoleBindingRequest(transIdRoleBinding, properties);
        String transIdCrnPattern =
                "crn://confluent.cloud/organization=orgId/environment=envId/cloud-cluster=clusterId/kafka=clusterId/transactional-id=myTransId";

        assertEquals(groupCrnPattern, groupRbRequest.crnPattern());
        assertEquals(transIdCrnPattern, transIdRbRequest.crnPattern());
    }

    static Stream<Resource.Metadata.Status> failedOrLegacyStatuses() {
        return Stream.of(Resource.Metadata.Status.ofFailed("error"), null);
    }
}
