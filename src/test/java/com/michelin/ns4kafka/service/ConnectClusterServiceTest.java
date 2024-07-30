package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.Assertions;
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
    void findAllEmpty() {
        when(connectClusterRepository.findAll()).thenReturn(List.of());

        StepVerifier.create(connectClusterService.findAll(false))
            .verifyComplete();
    }

    @Test
    void findAll() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(connectClusterRepository.findAll()).thenReturn(List.of(connectCluster));
        when(kafkaConnectClient.version(any(), any()))
            .thenReturn(Mono.just(HttpResponse.ok()));

        StepVerifier.create(connectClusterService.findAll(false))
            .consumeNextWith(result -> assertEquals(connectCluster, result))
            .verifyComplete();
    }

    @Test
    void shouldFindAllIncludingHardDeclared() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(connectClusterRepository.findAll()).thenReturn(new ArrayList<>(List.of(connectCluster)));
        ManagedClusterProperties kafka = new ManagedClusterProperties("local");
        kafka.setConnects(Map.of("test-connect", new ManagedClusterProperties.ConnectProperties()));
        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of(kafka));
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
    void findAllForNamespace() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        ConnectCluster connectClusterTwo = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix2.connect-two")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        ConnectCluster connectClusterThree = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix3.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        ConnectCluster connectClusterFour = ConnectCluster.builder()
            .metadata(Metadata.builder().name("not-owner")
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

        List<ConnectCluster> actual =
            connectClusterService.findAllByNamespace(namespace, List.of(AccessControlEntry.Permission.OWNER));

        assertEquals(2, actual.size());

        assertTrue(
            actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix.connect-cluster")));
        assertTrue(
            actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix2.connect-two")));

        Assertions.assertFalse(
            actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("not-owner")));
        Assertions.assertFalse(
            actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix3.connect-cluster")));
    }

    @Test
    void findByNamespaceAndName() {
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

        Optional<ConnectCluster> actual =
            connectClusterService.findByNamespaceAndNameOwner(namespace, "prefix.connect-cluster");

        assertTrue(actual.isPresent());
        assertEquals("prefix.connect-cluster", actual.get().getMetadata().getName());
        assertEquals(ConnectCluster.Status.HEALTHY, actual.get().getSpec().getStatus());
    }

    @Test
    void findByNamespaceAndNameUnhealthy() {
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

        Optional<ConnectCluster> actual =
            connectClusterService.findByNamespaceAndNameOwner(namespace, "prefix.connect-cluster");

        assertTrue(actual.isPresent());
        assertEquals("prefix.connect-cluster", actual.get().getMetadata().getName());
        assertEquals(ConnectCluster.Status.IDLE, actual.get().getSpec().getStatus());
        assertEquals("Internal Server Error", actual.get().getSpec().getStatusMessage());
    }

    @Test
    void findByNamespaceAndNameEmpty() {
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

        Optional<ConnectCluster> actual =
            connectClusterService.findByNamespaceAndNameOwner(namespace, "does-not-exist");

        assertTrue(actual.isEmpty());
    }

    @Test
    void create() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        when(connectClusterRepository.create(connectCluster)).thenReturn(connectCluster);

        ConnectCluster actual = connectClusterService.create(connectCluster);
        assertEquals(actual, connectCluster);
    }

    @Test
    void createCredentialsEncrypted() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("myUsername")
                .password("myPassword")
                .aes256Key("myAES256Key")
                .aes256Salt("myAES256Salt")
                .build())
            .build();

        when(connectClusterRepository.create(connectCluster)).thenReturn(connectCluster);
        when(securityProperties.getAes256EncryptionKey()).thenReturn("changeitchangeitchangeitchangeit");

        connectClusterService.create(connectCluster);

        assertNotEquals("myPassword", connectCluster.getSpec().getPassword());
        assertNotEquals("myAES256Key", connectCluster.getSpec().getAes256Key());
        assertNotEquals("myAES256Salt", connectCluster.getSpec().getAes256Salt());
    }

    @Test
    void validateConnectClusterCreationWhenNoNs4ConnectConfig() {
        ManagedClusterProperties kafka = new ManagedClusterProperties("local");
        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of(kafka));
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.just(ServerInfo.builder().build()));

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("test-connect")
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
    void validateConnectClusterCreationNotAlreadyDefined() {
        ManagedClusterProperties kafka = new ManagedClusterProperties("local");
        kafka.setConnects(Map.of("test-connect", new ManagedClusterProperties.ConnectProperties()));
        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of(kafka));
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.just(ServerInfo.builder().build()));

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("test-connect2")
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
    void validateConnectClusterCreationAlreadyDefined() {
        ManagedClusterProperties kafka = new ManagedClusterProperties("local");
        kafka.setConnects(Map.of("test-connect", new ManagedClusterProperties.ConnectProperties()));
        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of(kafka));
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
            .thenReturn(Mono.just(ServerInfo.builder().build()));

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("test-connect")
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
    void validateConnectClusterCreationDown() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("username")
                .password("password")
                .build())
            .build();

        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of());
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
    void validateConnectClusterCreationMalformedUrl() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("malformed-url")
                .build())
            .build();

        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of());

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
            .consumeNextWith(errors -> {
                assertEquals(1L, errors.size());
                assertEquals("Invalid value \"malformed-url\" for field \"url\": malformed URL.",
                    errors.getFirst());
            })
            .verifyComplete();
    }

    /**
     * Test validate connect cluster creation when aes 256 configuration missing salt.
     */
    @Test
    void validateConnectClusterCreationBadAes256MissingSalt() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("username")
                .password("password")
                .aes256Key("aes256Key")
                .build())
            .build();

        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of());
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

    /**
     * Test validate connect cluster creation when aes 256 configuration missing key.
     */
    @Test
    void validateConnectClusterCreationBadAes256MissingKey() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("username")
                .password("password")
                .aes256Salt("aes256Salt")
                .build())
            .build();

        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of());
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
    void validateConnectClusterCreationDownWithMissingKey() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("test-connect")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .username("username")
                .password("password")
                .aes256Salt("aes256Salt")
                .build())
            .build();

        when(managedClusterPropertiesList.stream()).thenReturn(Stream.of());
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
    void validateConnectClusterVaultNoClusterAvailable() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix.connect-cluster")
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
    void validateConnectClusterVaultNoClusterAvailableWithAes256() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster1 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix1.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();
        ConnectCluster connectCluster2 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix2.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("aes256Key")
                .build())
            .build();
        ConnectCluster connectCluster3 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix3.connect-cluster")
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

        List<String> errors =
            connectClusterService.validateConnectClusterVault(namespace, "prefix1.fake-connect-cluster");

        assertEquals(1L, errors.size());
        assertEquals("Invalid empty value for fields \"aes256Key, aes256Salt\": "
            + "AES key and salt are required to activate encryption.", errors.getFirst());
    }

    /**
     * Test validate connect cluster vault when Connect cluster required is not part of available list of
     * cluster with valid aes256 specs.
     */
    @Test
    void validateConnectClusterVaultClusterNotAvailable() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster1 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix1.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();
        ConnectCluster connectCluster2 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix2.connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("aes256Key")
                .aes256Salt("aes256Salt")
                .build())
            .build();
        ConnectCluster connectCluster3 = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix3.connect-cluster")
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

        List<String> errors =
            connectClusterService.validateConnectClusterVault(namespace, "prefix1.fake-connect-cluster");

        assertEquals(1L, errors.size());
        assertEquals("Invalid value \"prefix1.fake-connect-cluster\" for field \"name\": "
            + "value must be one of \"prefix2.connect-cluster, prefix3.connect-cluster\".", errors.getFirst());
    }

    @Test
    void validateConnectClusterVault() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix.connect-cluster")
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

        List<String> errors = connectClusterService.validateConnectClusterVault(namespace, "prefix.connect-cluster");

        assertEquals(0L, errors.size());
    }

    @Test
    void vaultPasswordNoConnectClusterWithAes256Config() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("prefix.connect-cluster")
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

    /**
     * Test vault password if no connect cluster with aes256 config define.
     */
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

        when(securityProperties.getAes256EncryptionKey()).thenReturn(encryptKey);
        List<ConnectCluster> actual = connectClusterService.findAllByNamespaceWrite(namespace);

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
    void shouldDelete() {
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
    void shouldNamespaceOwnerOfConnectCluster() {
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
    void shouldNamespaceAllowedForConnectCluster() {
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

        boolean actual = connectClusterService.isNamespaceAllowedForConnectCluster(namespace, "prefix.connect-cluster");

        assertTrue(actual);
    }

    @Test
    void shouldNamespaceNotAllowedForConnectCluster() {
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

        boolean actual =
            connectClusterService.isNamespaceAllowedForConnectCluster(namespace, "not-allowed-prefix.connect-cluster");

        Assertions.assertFalse(actual);
    }

    /**
     * Test vault password if no connect cluster with aes256 config define.
     */
    @Test
    void vaultPasswordWithoutFormat() {
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
            .metadata(Metadata.builder().name("prefix.connect-cluster")
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

        List<VaultResponse> actual =
            connectClusterService.vaultPassword(namespace, "prefix.connect-cluster", List.of("secret"));

        assertTrue(actual.getFirst().getSpec().getEncrypted().matches("^\\$\\{aes256\\:.*\\}"));
    }

    /**
     * Test vault password if no connect cluster with aes256 config define and format.
     */
    @Test
    void vaultPasswordWithFormat() {
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
            .metadata(Metadata.builder().name("prefix.connect-cluster")
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

        List<VaultResponse> actual =
            connectClusterService.vaultPassword(namespace, "prefix.connect-cluster", List.of("secret"));

        Assertions.assertFalse(actual.getFirst().getSpec().getEncrypted().matches("^\\$\\{aes256\\:.*\\}"));
    }
}
