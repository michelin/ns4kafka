package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.model.connect.cluster.VaultResponse;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.property.SecurityProperties;
import com.michelin.ns4kafka.repository.ConnectClusterRepository;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient;
import com.michelin.ns4kafka.service.client.connect.entities.ServerInfo;
import com.michelin.ns4kafka.util.EncryptionUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Connect cluster service test.
 */
@ExtendWith(MockitoExtension.class)
class ConnectClusterServiceTest {
    @Mock
    KafkaConnectClient kafkaConnectClient;

    @Mock
    ConnectClusterRepository connectClusterRepository;

    @Mock
    AclService aclService;

    @Mock
    List<ManagedClusterProperties> managedClusterPropertiesList;

    @Mock
    SecurityProperties securityProperties;

    @InjectMocks
    ConnectClusterService connectClusterService;

    @Mock
    HttpClient httpClient;

    @Test
    void shouldFindAllConnectClustersWhenEmpty() {
        when(connectClusterRepository.findAll())
            .thenReturn(List.of());

        StepVerifier.create(connectClusterService.findAll(false))
            .verifyComplete();
    }

    @Test
    void shouldFindAllConnectClustersExcludingThoseDeclaredInNs4KafkaConfig() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(connectClusterRepository.findAll())
            .thenReturn(List.of(connectCluster));
        when(kafkaConnectClient.version(any(), any()))
            .thenReturn(Mono.just(HttpResponse.ok()));

        StepVerifier.create(connectClusterService.findAll(false))
            .consumeNextWith(result -> assertEquals(connectCluster, result))
            .verifyComplete();
    }

    @Test
    void shouldFindAllConnectClustersIncludingThoseDeclaredInNs4KafkaConfig() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(connectClusterRepository.findAll())
            .thenReturn(new ArrayList<>(List.of(connectCluster)));

        ManagedClusterProperties kafka = new ManagedClusterProperties("local");
        kafka.setConnects(Map.of("test-connect", new ManagedClusterProperties.ConnectProperties()));

        when(managedClusterPropertiesList.stream())
            .thenReturn(Stream.of(kafka));
        when(kafkaConnectClient.version(any(), any()))
            .thenReturn(Mono.just(HttpResponse.ok()))
            .thenReturn(Mono.error(new Exception("error")));

        StepVerifier.create(connectClusterService.findAll(true))
            .consumeNextWith(result -> {
                assertEquals("connect-cluster", result.getMetadata().getName());
                assertEquals(ConnectCluster.Status.HEALTHY, result.getSpec().getStatus());
                assertNull(result.getSpec().getStatusMessage());
            })
            .consumeNextWith(result -> {
                assertEquals("test-connect", result.getMetadata().getName());
                assertEquals(ConnectCluster.Status.IDLE, result.getSpec().getStatus());
                assertEquals("error", result.getSpec().getStatusMessage());
            })
            .verifyComplete();
    }

    @Test
    void shouldFindAllConnectClustersForNamespaceWithOwnership() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        ConnectCluster connectClusterTwo = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix2.connect-two")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        ConnectCluster connectClusterThree = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix3.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        ConnectCluster connectClusterFour = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("not-owner")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster, connectClusterTwo, connectClusterThree, connectClusterFour));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix2.connect-two")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix3.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("topic.")
                        .build())
                    .build()
            ));
        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix2.connect-two"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix3.connect-cluster"))).thenReturn(false);
        when(aclService.isAnyAclOfResource(any(), eq("not-owner"))).thenReturn(false);

        assertEquals(List.of(connectCluster, connectClusterTwo), connectClusterService
            .findAllForNamespaceByPermissions(namespace, List.of(AccessControlEntry.Permission.OWNER)));
    }

    @Test
    void shouldListConnectClusterWithNameParameter() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder().build())
            .build();

        ConnectCluster cc1 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("abc.cc").build())
            .spec(ConnectCluster.ConnectClusterSpec.builder().url("https://after").build())
            .build();

        ConnectCluster cc2 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("xyz.connect-two").build())
            .spec(ConnectCluster.ConnectClusterSpec.builder().url("https://after").build())
            .build();

        List<AccessControlEntry> acls = List.of(
            AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .permission(AccessControlEntry.Permission.OWNER)
                    .grantedTo("namespace")
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                    .resource("abc.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .permission(AccessControlEntry.Permission.OWNER)
                    .grantedTo("namespace")
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                    .resource("xyz.connect-two")
                    .build())
                .build()
        );

        when(connectClusterRepository.findAllForCluster("local")).thenReturn(List.of(cc1, cc2));
        when(aclService.findAllGrantedToNamespace(namespace)).thenReturn(acls);
        when(aclService.isAnyAclOfResource(acls, "abc.cc")).thenReturn(true);
        when(aclService.isAnyAclOfResource(acls, "xyz.connect-two")).thenReturn(true);
        when(kafkaConnectClient.version(any(), any())).thenReturn(Mono.just(HttpResponse.ok()));

        assertEquals(List.of(cc1), connectClusterService.findByWildcardNameWithOwnerPermission(namespace, "abc.cc"));
        assertTrue(connectClusterService.findByWildcardNameWithOwnerPermission(namespace, "not-owner").isEmpty());
    }

    @Test
    void listConnectClusterWithWildcardNameParameter() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder().build())
            .build();

        ConnectCluster cc1 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix.cc1").build())
            .spec(ConnectCluster.ConnectClusterSpec.builder().url("https://after").build())
            .build();

        ConnectCluster cc2 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix.cc2").build())
            .spec(ConnectCluster.ConnectClusterSpec.builder().url("https://after").build())
            .build();

        ConnectCluster cc3 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix2.connect-two").build())
            .spec(ConnectCluster.ConnectClusterSpec.builder().url("https://after").build())
            .build();

        ConnectCluster cc4 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix3.connect1").build())
            .spec(ConnectCluster.ConnectClusterSpec.builder().url("https://after").build())
            .build();

        ConnectCluster cc5 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix3.connect2").build())
            .spec(ConnectCluster.ConnectClusterSpec.builder().url("https://after").build())
            .build();

        List<AccessControlEntry> acls = List.of(
            AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .permission(AccessControlEntry.Permission.OWNER)
                    .grantedTo("namespace")
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                    .resource("prefix.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .permission(AccessControlEntry.Permission.OWNER)
                    .grantedTo("namespace")
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                    .resource("prefix3.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .permission(AccessControlEntry.Permission.OWNER)
                    .grantedTo("namespace")
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                    .resource("prefix2.connect-two")
                    .build())
                .build()
        );

        when(connectClusterRepository.findAllForCluster("local")).thenReturn(List.of(cc1, cc2, cc3, cc4, cc5));
        when(aclService.findAllGrantedToNamespace(namespace)).thenReturn(acls);
        when(aclService.isAnyAclOfResource(any(), eq("prefix.cc1"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix.cc2"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix2.connect-two"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix3.connect1"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix3.connect2"))).thenReturn(true);
        when(kafkaConnectClient.version(any(), any())).thenReturn(Mono.just(HttpResponse.ok()));

        assertEquals(List.of(cc1, cc2, cc3, cc4, cc5), connectClusterService
            .findByWildcardNameWithOwnerPermission(namespace, "*"));
        assertEquals(List.of(cc1, cc2), connectClusterService
            .findByWildcardNameWithOwnerPermission(namespace, "prefix.*"));
        assertEquals(List.of(cc4, cc5), connectClusterService
            .findByWildcardNameWithOwnerPermission(namespace,  "prefix?.connect?"));
        assertEquals(List.of(cc2, cc5), connectClusterService
            .findByWildcardNameWithOwnerPermission(namespace, "*2"));
        assertEquals(List.of(cc3), connectClusterService
            .findByWildcardNameWithOwnerPermission(namespace, "prefix*.*-two"));
        assertTrue(connectClusterService.findByWildcardNameWithOwnerPermission(namespace, "*-three").isEmpty());
        assertTrue(connectClusterService.findByWildcardNameWithOwnerPermission(namespace, "prefix?.cc?").isEmpty());
    }

    @Test
    void shouldFindAllConnectClustersWithWritePermissionAndHideCredentialsWhenNotOwner() {
        String encryptKey = "changeitchangeitchangeitchangeit";
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .password(EncryptionUtils.encryptAes256Gcm("password", encryptKey))
                .aes256Key(EncryptionUtils.encryptAes256Gcm("aes256Key", encryptKey))
                .aes256Salt(EncryptionUtils.encryptAes256Gcm("aes256Salt", encryptKey))
                .build())
            .build();

        ConnectCluster connectClusterOwner = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("owner.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .password(EncryptionUtils.encryptAes256Gcm("password", encryptKey))
                .aes256Key(EncryptionUtils.encryptAes256Gcm("aes256Key", encryptKey))
                .aes256Salt(EncryptionUtils.encryptAes256Gcm("aes256Salt", encryptKey))
                .build())
            .build();

        AccessControlEntry acl1 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .permission(AccessControlEntry.Permission.WRITE)
                .grantedTo("namespace")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                .resource("prefix.")
                .build())
            .build();

        AccessControlEntry acl2 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .permission(AccessControlEntry.Permission.OWNER)
                .grantedTo("namespace")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                .resource("owner.")
                .build())
            .build();

        when(kafkaConnectClient.version(any(), any()))
            .thenReturn(Mono.just(HttpResponse.ok()));

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster, connectClusterOwner));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(acl1, acl2));

        when(securityProperties.getAes256EncryptionKey()).thenReturn(encryptKey);
        when(aclService.isAnyAclOfResource(List.of(acl1), "prefix.connect-cluster")).thenReturn(true);
        when(aclService.isAnyAclOfResource(List.of(acl2), "prefix.connect-cluster")).thenReturn(false);
        when(aclService.isAnyAclOfResource(List.of(acl2), "owner.connect-cluster")).thenReturn(true);
        when(aclService.isAnyAclOfResource(List.of(acl1), "owner.connect-cluster")).thenReturn(false);

        List<ConnectCluster> actual = connectClusterService.findAllForNamespaceWithWritePermission(namespace);

        assertEquals(2, actual.size());

        assertEquals("password", actual.getFirst().getSpec().getPassword());
        assertEquals("aes256Key", actual.getFirst().getSpec().getAes256Key());
        assertEquals("aes256Salt", actual.getFirst().getSpec().getAes256Salt());

        assertEquals("*****", actual.get(1).getSpec().getPassword());
        assertEquals("*****", actual.get(1).getSpec().getAes256Key());
        assertEquals("*****", actual.get(1).getSpec().getAes256Salt());
    }

    @Test
    void shouldFindConnectClusterByNamespaceAndName() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(kafkaConnectClient.version("local", "prefix.connect-cluster"))
            .thenReturn(Mono.just(HttpResponse.ok()));

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build()
            ));

        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);

        Optional<ConnectCluster> actual =
            connectClusterService.findByNameWithOwnerPermission(namespace, "prefix.connect-cluster");

        assertTrue(actual.isPresent());
        assertEquals("prefix.connect-cluster", actual.get().getMetadata().getName());
        assertEquals(ConnectCluster.Status.HEALTHY, actual.get().getSpec().getStatus());
    }

    @Test
    void shouldFindConnectClusterByNamespaceAndNameWhenStatusIsUnhealthy() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(kafkaConnectClient.version("local", "prefix.connect-cluster"))
            .thenReturn(Mono.error(new HttpClientException("Internal Server Error")));

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build()
            ));

        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);

        Optional<ConnectCluster> actual =
            connectClusterService.findByNameWithOwnerPermission(namespace, "prefix.connect-cluster");

        assertTrue(actual.isPresent());
        assertEquals("prefix.connect-cluster", actual.get().getMetadata().getName());
        assertEquals(ConnectCluster.Status.IDLE, actual.get().getSpec().getStatus());
        assertEquals("Internal Server Error", actual.get().getSpec().getStatusMessage());
    }

    @Test
    void shouldNotFindConnectClusterByNamespaceAndNameWhenDoesNotExist() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build()
            ));

        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);

        assertTrue(connectClusterService.findByNameWithOwnerPermission(namespace, "does-not-exist").isEmpty());
    }

    @Test
    void shouldCreateConnectCluster() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(connectClusterRepository.create(connectCluster)).thenReturn(connectCluster);

        ConnectCluster actual = connectClusterService.create(connectCluster);
        assertEquals(connectCluster, actual);
    }

    @Test
    void shouldCreateConnectClusterWithEncryptedCredentials() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("myUsername")
                .password("myPassword")
                .aes256Key("myAES256Key")
                .aes256Salt("myAES256Salt")
                .build())
            .build();

        when(connectClusterRepository.create(connectCluster))
                .thenReturn(connectCluster);
        when(securityProperties.getAes256EncryptionKey())
                .thenReturn("changeitchangeitchangeitchangeit");

        ConnectCluster actual = connectClusterService.create(connectCluster);

        assertNotEquals("myPassword", actual.getSpec().getPassword());
        assertNotEquals("myAES256Key", actual.getSpec().getAes256Key());
        assertNotEquals("myAES256Salt", actual.getSpec().getAes256Salt());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidateConnectClusterCreationWhenNs4KafkaConnectClustersConfigIsNull() {
        ManagedClusterProperties kafka = new ManagedClusterProperties("local");

        when(managedClusterPropertiesList.stream())
            .thenReturn(Stream.of(kafka));
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.just(ServerInfo.builder().build()));

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidateConnectClusterCreationWhenNotAlreadyDefined() {
        ManagedClusterProperties kafka = new ManagedClusterProperties("local");
        kafka.setConnects(Map.of("test-connect", new ManagedClusterProperties.ConnectProperties()));

        when(managedClusterPropertiesList.stream())
            .thenReturn(Stream.of(kafka));
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.just(ServerInfo.builder().build()));

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("test-connect2")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidateConnectClusterCreationWhenAlreadyDefined() {
        ManagedClusterProperties kafka = new ManagedClusterProperties("local");
        kafka.setConnects(Map.of("test-connect", new ManagedClusterProperties.ConnectProperties()));

        when(managedClusterPropertiesList.stream())
            .thenReturn(Stream.of(kafka));
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.just(ServerInfo.builder().build()));

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> {
                assertEquals(1L, errors.size());
                assertEquals(
                    "Invalid value \"test-connect\" for field \"name\": a Kafka Connect is already defined "
                        + "globally with this name. Please provide a different name.",
                    errors.getFirst());
            })
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidateConnectClusterCreationWhenDown() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("username")
                .password("password")
                .build())
            .build();

        when(managedClusterPropertiesList.stream())
                .thenReturn(Stream.of());
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.error(new HttpClientException("Error")));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> {
                assertEquals(1L, errors.size());
                assertEquals("Invalid \"test-connect\": the Kafka Connect is not healthy (error).", errors.getFirst());
            })
            .verifyComplete();
    }

    @Test
    void shouldValidateConnectClusterCreationWhenMalformedUrl() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("malformed-url")
                .build())
            .build();

        when(managedClusterPropertiesList.stream())
                .thenReturn(Stream.of());

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> {
                assertEquals(1L, errors.size());
                assertEquals("Invalid value \"malformed-url\" for field \"url\": malformed URL.",
                    errors.getFirst());
            })
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidateConnectClusterCreationWhenBadAes256MissingSalt() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("username")
                .password("password")
                .aes256Key("aes256Key")
                .build())
            .build();

        when(managedClusterPropertiesList.stream())
                .thenReturn(Stream.of());
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.just(ServerInfo.builder().build()));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> {
                assertEquals(1L, errors.size());
                assertEquals("Invalid empty value for fields \"aes256Key, aes256Salt\": "
                        + "AES key and salt are required to activate encryption.",
                    errors.getFirst());
            })
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidateConnectClusterCreationWhenBadAes256MissingKey() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("username")
                .password("password")
                .aes256Salt("aes256Salt")
                .build())
            .build();

        when(managedClusterPropertiesList.stream())
            .thenReturn(Stream.of());
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.just(ServerInfo.builder().build()));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> {
                assertEquals(1L, errors.size());
                assertEquals("Invalid empty value for fields \"aes256Key, aes256Salt\": "
                        + "AES key and salt are required to activate encryption.",
                    errors.getFirst());
            })
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidateConnectClusterCreationWhenDownAndMissingKey() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("username")
                .password("password")
                .aes256Salt("aes256Salt")
                .build())
            .build();

        when(managedClusterPropertiesList.stream())
            .thenReturn(Stream.of());
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.error(new HttpClientException("Error")));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> {
                assertEquals(2L, errors.size());
                assertTrue(errors.contains("Invalid \"test-connect\": the Kafka Connect is not healthy (error)."));
                assertTrue(errors.contains("Invalid empty value for fields \"aes256Key, aes256Salt\": "
                    + "AES key and salt are required to activate encryption."));
            })
            .verifyComplete();
    }

    @Test
    void shouldValidateConnectClusterVaultWhenNoClusterAvailable() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("aes256Key")
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("fake-prefix.")
                        .build())
                    .build()
            ));

        List<String> errors =
            connectClusterService.validateConnectClusterVault(namespace, "prefix.fake-connect-cluster");

        assertEquals(1L, errors.size());
        assertEquals("Invalid value \"prefix.fake-connect-cluster\" for field \"name\": resource not found.",
            errors.getFirst());
    }

    @Test
    void shouldValidateConnectClusterVaultWhenNoClusterAvailableWithAes256() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster1 = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix1.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster2 = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix2.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("aes256Key")
                .build())
            .build();

        ConnectCluster connectCluster3 = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix3.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Salt("aes256Salt")
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster1, connectCluster2, connectCluster3));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix1.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix2.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix3.")
                        .build())
                    .build()
            ));

        when(aclService.isAnyAclOfResource(any(), eq("prefix1.connect-cluster"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix2.connect-cluster"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix3.connect-cluster"))).thenReturn(true);

        List<String> errors =
            connectClusterService.validateConnectClusterVault(namespace, "prefix1.fake-connect-cluster");

        assertEquals(1L, errors.size());
        assertEquals("Invalid empty value for fields \"aes256Key, aes256Salt\": "
            + "AES key and salt are required to activate encryption.", errors.getFirst());
    }

    @Test
    void shouldValidateConnectClusterVaultWhenClusterNotAvailable() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster1 = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix1.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster2 = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix2.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("aes256Key")
                .aes256Salt("aes256Salt")
                .build())
            .build();

        ConnectCluster connectCluster3 = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix3.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("aes256Key")
                .aes256Salt("aes256Salt")
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster1, connectCluster2, connectCluster3));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix1.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix2.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix3.")
                        .build())
                    .build()
            ));

        when(aclService.isAnyAclOfResource(any(), eq("prefix1.connect-cluster"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix2.connect-cluster"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("prefix3.connect-cluster"))).thenReturn(true);

        List<String> errors =
            connectClusterService.validateConnectClusterVault(namespace, "prefix1.fake-connect-cluster");

        assertEquals(1L, errors.size());
        assertEquals("Invalid value \"prefix1.fake-connect-cluster\" for field \"name\": "
            + "value must be one of \"prefix2.connect-cluster, prefix3.connect-cluster\".", errors.getFirst());
    }

    @Test
    void shouldValidateConnectClusterVault() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("aes256Key")
                .aes256Salt("aes256Salt")
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build()
            ));

        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);

        List<String> errors = connectClusterService.validateConnectClusterVault(namespace, "prefix.connect-cluster");

        assertEquals(0L, errors.size());
    }

    @Test
    void shouldNotVaultPasswordWhenConnectClusterMissesAes256Config() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("aes256Key")
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build()
            ));

        List<VaultResponse> actual =
            connectClusterService.vaultPassword(namespace, "prefix.connect-cluster", List.of("secret"));

        assertEquals("secret", actual.getFirst().getSpec().getEncrypted());
    }

    @Test
    void findAllByNamespaceWriteAsOwner() {
        String encryptKey = "changeitchangeitchangeitchangeit";
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(Metadata.builder()
                        .name("prefix.connect-cluster")
                        .cluster("local")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .password(EncryptionUtils.encryptAes256Gcm("password", encryptKey))
                        .aes256Key(EncryptionUtils.encryptAes256Gcm("aes256Key", encryptKey))
                        .aes256Salt(EncryptionUtils.encryptAes256Gcm("aes256Salt", encryptKey))
                        .build())
                .build();

        ConnectCluster connectClusterOwner = ConnectCluster.builder()
                .metadata(Metadata.builder()
                        .name("owner.connect-cluster")
                        .cluster("local")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .password(EncryptionUtils.encryptAes256Gcm("password", encryptKey))
                        .aes256Key(EncryptionUtils.encryptAes256Gcm("aes256Key", encryptKey))
                        .aes256Salt(EncryptionUtils.encryptAes256Gcm("aes256Salt", encryptKey))
                        .build())
                .build();

        AccessControlEntry acl1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                .build();

        AccessControlEntry acl2 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("owner.")
                        .build())
                .build();


        when(aclService.findAllGrantedToNamespace(namespace)).thenReturn(List.of(acl1, acl2));
        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster, connectClusterOwner));
        when(aclService.isAnyAclOfResource(List.of(acl2), "owner.connect-cluster")).thenReturn(true);
        when(aclService.isAnyAclOfResource(List.of(acl2), "prefix.connect-cluster")).thenReturn(false);
        when(securityProperties.getAes256EncryptionKey()).thenReturn(encryptKey);
        when(kafkaConnectClient.version(any(), any())).thenReturn(Mono.just(HttpResponse.ok()));
        when(aclService.isAnyAclOfResource(List.of(acl1), "prefix.connect-cluster")).thenReturn(true);
        when(aclService.isAnyAclOfResource(List.of(acl1), "owner.connect-cluster")).thenReturn(false);

        List<ConnectCluster> actual = connectClusterService.findAllForNamespaceWithWritePermission(namespace);

        assertEquals(2, actual.size());
        // 1rts is for owner with decrypted values
        assertEquals("password", actual.getFirst().getSpec().getPassword());
        assertEquals("aes256Key", actual.getFirst().getSpec().getAes256Key());
        assertEquals("aes256Salt", actual.getFirst().getSpec().getAes256Salt());

        // second is only for write with wildcards
        assertEquals("*****", actual.get(1).getSpec().getPassword());
        assertEquals("*****", actual.get(1).getSpec().getAes256Key());
        assertEquals("*****", actual.get(1).getSpec().getAes256Salt());
    }

    @Test
    void shouldDeleteConnectCluster() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        connectClusterService.delete(connectCluster);

        verify(connectClusterRepository).delete(connectCluster);
    }

    @Test
    void shouldValidateNamespaceOwnerOfConnectCluster() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        when(aclService.isNamespaceOwnerOfResource(any(), any(), any()))
            .thenReturn(true);

        boolean actual = connectClusterService.isNamespaceOwnerOfConnectCluster(namespace, "prefix.connect-cluster");

        assertTrue(actual);
    }

    @Test
    void shouldNamespaceBeAllowedToWriteToConnectCluster() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        ConnectCluster connectClusterOwner = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("owner.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        when(kafkaConnectClient.version(any(), any()))
            .thenReturn(Mono.just(HttpResponse.ok()));

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster, connectClusterOwner));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("owner.")
                        .build())
                    .build()
            ));

        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);

        assertTrue(connectClusterService.isNamespaceAllowedForConnectCluster(namespace, "prefix.connect-cluster"));
    }

    @Test
    void shouldNamespaceNotBeAllowedToWriteToConnectCluster() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        ConnectCluster connectClusterOwner = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("owner.connect-cluster")
                .cluster("local")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        when(kafkaConnectClient.version(any(), any()))
            .thenReturn(Mono.just(HttpResponse.ok()));

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster, connectClusterOwner));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("owner.")
                        .build())
                    .build()
            ));

        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);
        when(aclService.isAnyAclOfResource(any(), eq("owner.connect-cluster"))).thenReturn(true);

        assertFalse(connectClusterService.isNamespaceAllowedForConnectCluster(namespace, "not-allowed-prefix.cc"));
    }

    @Test
    void shouldVaultPasswordWithoutFormat() {
        String encryptionKey = "changeitchangeitchangeitchangeit";

        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key(EncryptionUtils.encryptAes256Gcm("aes256Key", encryptionKey))
                .aes256Salt(EncryptionUtils.encryptAes256Gcm("aes256Salt", encryptionKey))
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build()
            ));

        when(securityProperties.getAes256EncryptionKey()).thenReturn("changeitchangeitchangeitchangeit");
        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);

        List<VaultResponse> actual =
            connectClusterService.vaultPassword(namespace, "prefix.connect-cluster", List.of("secret"));

        assertTrue(actual.getFirst().getSpec().getEncrypted().matches("^\\$\\{aes256:.*}"));
    }

    @Test
    void shouldVaultPasswordWithFormat() {
        String encryptionKey = "changeitchangeitchangeitchangeit";

        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key(EncryptionUtils.encryptAes256Gcm("aes256Key", encryptionKey))
                .aes256Salt(EncryptionUtils.encryptAes256Gcm("aes256Salt", encryptionKey))
                .aes256Format("%s")
                .build())
            .build();

        when(connectClusterRepository.findAllForCluster("local"))
            .thenReturn(List.of(connectCluster));

        when(aclService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                        .resource("prefix.")
                        .build())
                    .build()
            ));

        when(securityProperties.getAes256EncryptionKey()).thenReturn("changeitchangeitchangeitchangeit");
        when(aclService.isAnyAclOfResource(any(), eq("prefix.connect-cluster"))).thenReturn(true);

        List<VaultResponse> actual =
            connectClusterService.vaultPassword(namespace, "prefix.connect-cluster", List.of("secret"));

        assertFalse(actual.getFirst().getSpec().getEncrypted().matches("^\\$\\{aes256:.*}"));
    }
}
