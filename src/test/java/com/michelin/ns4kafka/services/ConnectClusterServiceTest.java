package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.models.connect.cluster.VaultResponse;
import com.michelin.ns4kafka.repositories.ConnectClusterRepository;
import com.michelin.ns4kafka.services.clients.connect.KafkaConnectClient;
import com.michelin.ns4kafka.services.clients.connect.entities.ServerInfo;
import com.michelin.ns4kafka.utils.EncryptionUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConnectClusterServiceTest {
    @Mock
    KafkaConnectClient kafkaConnectClient;

    @Mock
    ConnectClusterRepository connectClusterRepository;

    @Mock
    AccessControlEntryService accessControlEntryService;

    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;

    @Mock
    SecurityConfig securityConfig;

    @InjectMocks
    ConnectClusterService connectClusterService;

    @Mock
    HttpClient httpClient;

    /**
     * Test find all
     */
    @Test
    void findAllEmpty() {
        when(connectClusterRepository.findAll()).thenReturn(List.of());
        
        StepVerifier.create(connectClusterService.findAll(false))
            .verifyComplete();
    }

    /**
     * Test find all
     */
    @Test
    void findAll() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        when(connectClusterRepository.findAll()).thenReturn(List.of(connectCluster));
        
        StepVerifier.create(connectClusterService.findAll(false))
            .consumeNextWith(result -> assertEquals(connectCluster, result))
            .verifyComplete();
    }

    /**
     * Test find all
     */
    @Test
    void shouldFindAllIncludingHardDeclared() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        when(connectClusterRepository.findAll()).thenReturn(List.of(connectCluster));
        KafkaAsyncExecutorConfig kafka = new KafkaAsyncExecutorConfig("local");
        kafka.setConnects(Map.of("test-connect", new KafkaAsyncExecutorConfig.ConnectConfig()));
        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of(kafka));
        when(kafkaConnectClient.version(any(), any()))
                .thenReturn(Mono.just(HttpResponse.ok()))
                .thenReturn(Mono.error(new Exception("error")));

        StepVerifier.create(connectClusterService.findAll(true))
                .consumeNextWith(result -> {
                    assertEquals("connect-cluster", result.getMetadata().getName());
                    assertEquals(ConnectCluster.Status.HEALTHY, result.getSpec().getStatus());
                    assertTrue(result.getSpec().getStatusMessage().isEmpty());
                })
                .consumeNextWith(result -> {
                    assertEquals("test-connect", result.getMetadata().getName());
                    assertEquals(ConnectCluster.Status.IDLE, result.getSpec().getStatus());
                    assertEquals("error", result.getSpec().getStatusMessage());
                })
                .verifyComplete();
    }

    /**
     * Test find all for namespace
     */
    @Test
    void findAllForNamespace() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        ConnectCluster connectClusterTwo = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix2.connect-two")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        ConnectCluster connectClusterThree = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix3.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        ConnectCluster connectClusterFour = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("not-owner")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster, connectClusterTwo, connectClusterThree, connectClusterFour));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        List<ConnectCluster> actual = connectClusterService.findAllByNamespace(namespace, List.of(AccessControlEntry.Permission.OWNER));

        assertEquals(2, actual.size());
    
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix.connect-cluster")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix2.connect-two")));
     
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("not-owner")));
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix3.connect-cluster")));
    }

    /**
     * Test find by namespace and name
     */
    @Test
    void findByNamespaceAndName() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
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

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        Optional<ConnectCluster> actual = connectClusterService.findByNamespaceAndNameOwner(namespace, "prefix.connect-cluster");

        Assertions.assertTrue(actual.isPresent());
        assertEquals("prefix.connect-cluster", actual.get().getMetadata().getName());
        assertEquals(ConnectCluster.Status.HEALTHY, actual.get().getSpec().getStatus());
    }

    /**
     * Test find by namespace and name when Kafka Connect is unhealthy
     */
    @Test
    void findByNamespaceAndNameUnhealthy() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
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

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        Optional<ConnectCluster> actual = connectClusterService.findByNamespaceAndNameOwner(namespace, "prefix.connect-cluster");

        Assertions.assertTrue(actual.isPresent());
        assertEquals("prefix.connect-cluster", actual.get().getMetadata().getName());
        assertEquals(ConnectCluster.Status.IDLE, actual.get().getSpec().getStatus());
        assertEquals("Internal Server Error", actual.get().getSpec().getStatusMessage());
    }

    /**
     * Test find by namespace and name empty response
     */
    @Test
    void findByNamespaceAndNameEmpty() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
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

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        Optional<ConnectCluster> actual = connectClusterService.findByNamespaceAndNameOwner(namespace, "does-not-exist");

        Assertions.assertTrue(actual.isEmpty());
    }

    /**
     * Test creation
     */
    @Test
    void create() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        when(connectClusterRepository.create(connectCluster)).thenReturn(connectCluster);

        ConnectCluster actual = connectClusterService.create(connectCluster);
        assertEquals(actual, connectCluster);
    }


    /**
     * Test creation with encrypted credentials
     */
    @Test
    void createCredentialsEncrypted() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
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
        when(securityConfig.getAes256EncryptionKey()).thenReturn("changeitchangeitchangeitchangeit");

        connectClusterService.create(connectCluster);

        assertNotEquals("myPassword", connectCluster.getSpec().getPassword());
        assertNotEquals("myAES256Key", connectCluster.getSpec().getAes256Key());
        assertNotEquals("myAES256Salt", connectCluster.getSpec().getAes256Salt());
    }

    /**
     * Test validate connect cluster creation when Connect cluster is already defined in the
     * Ns4Kafka configuration
     */
    @Test
    void validateConnectClusterCreationAlreadyDefined() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("test-connect")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        KafkaAsyncExecutorConfig kafka = new KafkaAsyncExecutorConfig("local");
        kafka.setConnects(Map.of("test-connect", new KafkaAsyncExecutorConfig.ConnectConfig()));
        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of(kafka));
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
                .thenReturn(Mono.just(ServerInfo.builder().build()));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
                .consumeNextWith(errors -> {
                    assertEquals(1L, errors.size());
                    assertEquals("A Kafka Connect is already defined globally with the name \"test-connect\". Please provide a different name.", errors.get(0));
                })
                .verifyComplete();
    }

    /**
     * Test validate connect cluster creation when Connect cluster is down
     */
    @Test
    void validateConnectClusterCreationDown() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("test-connect")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .username("username")
                        .password("password")
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of());
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
                .thenReturn(Mono.error(new HttpClientException("Error")));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
                .consumeNextWith(errors -> {
                    assertEquals(1L, errors.size());
                    assertEquals("The Kafka Connect \"test-connect\" is not healthy (Error).", errors.get(0));
                })
                .verifyComplete();
    }

    /**
     * Test validate connect cluster creation malformed URL
     */
    @Test
    void validateConnectClusterCreationMalformedUrl() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("test-connect")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("malformed-url")
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of());

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
                .consumeNextWith(errors -> {
                    assertEquals(1L, errors.size());
                    assertEquals("The Kafka Connect \"test-connect\" has a malformed URL \"malformed-url\".", errors.get(0));
                })
                .verifyComplete();
    }

    /**
     * Test validate connect cluster creation when aes 256 configuration missing salt.
     */
    @Test
    void validateConnectClusterCreationBadAES256MissingSalt() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("test-connect")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .username("username")
                        .password("password")
                        .aes256Key("aes256Key")
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of());
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
                .thenReturn(Mono.just(ServerInfo.builder().build()));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
                .consumeNextWith(errors -> {
                    assertEquals(1L, errors.size());
                    assertEquals("The Connect cluster \"test-connect\" \"aes256Key\" and \"aes256Salt\" specs are required to activate the encryption.", errors.get(0));
                })
                .verifyComplete();
    }

    /**
     * Test validate connect cluster creation when aes 256 configuration missing key.
     */
    @Test
    void validateConnectClusterCreationBadAES256MissingKey() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("test-connect")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .username("username")
                        .password("password")
                        .aes256Salt("aes256Salt")
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of());
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
                .thenReturn(Mono.just(ServerInfo.builder().build()));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
                .consumeNextWith(errors -> {
                    assertEquals(1L, errors.size());
                    assertEquals("The Connect cluster \"test-connect\" \"aes256Key\" and \"aes256Salt\" specs are required to activate the encryption.", errors.get(0));
                })
                .verifyComplete();
    }

    /**
     * Test validate connect cluster creation when Connect cluster is down and encryption key is missing
     */
    @Test
    void validateConnectClusterCreationDownWithMissingKey() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("test-connect")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .username("username")
                        .password("password")
                        .aes256Salt("aes256Salt")
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of());
        when(httpClient.retrieve(any(MutableHttpRequest.class), eq(ServerInfo.class)))
                .thenReturn(Mono.error(new HttpClientException("Error")));

        StepVerifier.create(connectClusterService.validateConnectClusterCreation(connectCluster))
                .consumeNextWith(errors -> {
                    assertEquals(2L, errors.size());
                    assertTrue(errors.contains("The Kafka Connect \"test-connect\" is not healthy (Error)."));
                    assertTrue(errors.contains("The Connect cluster \"test-connect\" \"aes256Key\" and \"aes256Salt\" specs are required to activate the encryption."));
                })
                .verifyComplete();
    }

    /**
     * Test validate connect cluster vault when No Connect cluster are available for namespace
     */
    @Test
    void validateConnectClusterVaultNoClusterAvailable() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key("aes256Key")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        List<String> errors = connectClusterService.validateConnectClusterVault(namespace, "prefix.fake-connect-cluster");

        assertEquals(1L, errors.size());
        assertEquals("No Connect Cluster available.", errors.get(0));
    }

    /**
     * Test validate connect cluster vault when namespace does not have any Connect Cluster
     * with valid aes256 specs.
     */
    @Test
    void validateConnectClusterVaultNoClusterAvailableWithAES256() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster1 = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix1.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .build())
                .build();
        ConnectCluster connectCluster2 = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix2.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key("aes256Key")
                        .build())
                .build();
        ConnectCluster connectCluster3 = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix3.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Salt("aes256Salt")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster1, connectCluster2, connectCluster3));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        List<String> errors = connectClusterService.validateConnectClusterVault(namespace, "prefix1.fake-connect-cluster");

        assertEquals(1L, errors.size());
        assertEquals("No Connect cluster available with valid aes256 specs configuration.", errors.get(0));
    }

    /**
     * Test validate connect cluster vault when Connect cluster required is not part of available list of
     * cluster with valid aes256 specs.
     */
    @Test
    void validateConnectClusterVaultClusterNotAvailable() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster1 = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix1.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .build())
                .build();
        ConnectCluster connectCluster2 = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix2.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key("aes256Key")
                        .aes256Salt("aes256Salt")
                        .build())
                .build();
        ConnectCluster connectCluster3 = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix3.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key("aes256Key")
                        .aes256Salt("aes256Salt")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster1, connectCluster2, connectCluster3));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        List<String> errors = connectClusterService.validateConnectClusterVault(namespace, "prefix1.fake-connect-cluster");

        assertEquals(1L, errors.size());
        assertEquals("Invalid value \"prefix1.fake-connect-cluster\" for Connect Cluster: Value must be one of [" +
                "prefix2.connect-cluster, prefix3.connect-cluster].", errors.get(0));
    }

    /**
     * Test validate connect cluster vault.
     */
    @Test
    void validateConnectClusterVault() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key("aes256Key")
                        .aes256Salt("aes256Salt")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

    /**
     * Test vault password if no connect cluster with aes256 config define.
     */
    @Test
    void vaultPasswordNoConnectClusterWithAes256Config() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key("aes256Key")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        List<VaultResponse> actual = connectClusterService.vaultPassword(namespace, "prefix.connect-cluster", List.of("secret"));

        assertEquals("secret", actual.get(0).getSpec().getEncrypted());
    }

    /**
     * Test vault password if no connect cluster with aes256 config define.
     */
    @Test
    void findAllByNamespaceWriteAsOwner() {
        String encryptKey = "changeitchangeitchangeitchangeit";
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.connect-cluster")
                        .cluster("local")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .password(EncryptionUtils.encryptAES256GCM("password", encryptKey))
                        .aes256Key(EncryptionUtils.encryptAES256GCM("aes256Key", encryptKey))
                        .aes256Salt(EncryptionUtils.encryptAES256GCM("aes256Salt", encryptKey))
                        .build())
                .build();

        ConnectCluster connectClusterOwner = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
                        .name("owner.connect-cluster")
                        .cluster("local")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .password(EncryptionUtils.encryptAES256GCM("password", encryptKey))
                        .aes256Key(EncryptionUtils.encryptAES256GCM("aes256Key", encryptKey))
                        .aes256Salt(EncryptionUtils.encryptAES256GCM("aes256Salt", encryptKey))
                        .build())
                .build();

        when(kafkaConnectClient.version(any(), any()))
                .thenReturn(Mono.just(HttpResponse.ok()));

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster, connectClusterOwner));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        when(securityConfig.getAes256EncryptionKey()).thenReturn(encryptKey);
        List<ConnectCluster> actual = connectClusterService.findAllByNamespaceWrite(namespace);

        assertEquals(2, actual.size());
        // 1rts is for owner with decrypted values
        assertEquals("password", actual.get(0).getSpec().getPassword());
        assertEquals("aes256Key", actual.get(0).getSpec().getAes256Key());
        assertEquals("aes256Salt", actual.get(0).getSpec().getAes256Salt());

        // second is only for write with wildcards
        assertEquals("*****", actual.get(1).getSpec().getPassword());
        assertEquals("*****", actual.get(1).getSpec().getAes256Key());
        assertEquals("*****", actual.get(1).getSpec().getAes256Salt());
    }

    /**
     * Should delete a self-deployed Kafka Connect
     */
    @Test
    void shouldDelete() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.connect-cluster")
                        .cluster("local")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .build())
                .build();

        connectClusterService.delete(connectCluster);

        verify(connectClusterRepository).delete(connectCluster);
    }

    /**
     * Should namespace be owner of Kafka Connect
     */
    @Test
    void shouldNamespaceOwnerOfConnectCluster() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        when(accessControlEntryService.isNamespaceOwnerOfResource(any(), any(), any()))
                .thenReturn(true);

        boolean actual = connectClusterService.isNamespaceOwnerOfConnectCluster(namespace, "prefix.connect-cluster");

        Assertions.assertTrue(actual);
    }

    @Test
    void shouldNamespaceAllowedForConnectCluster() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.connect-cluster")
                        .cluster("local")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .build())
                .build();

        ConnectCluster connectClusterOwner = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
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

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        Assertions.assertTrue(actual);
    }

    @Test
    void shouldNamespaceNotAllowedForConnectCluster() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.connect-cluster")
                        .cluster("local")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .build())
                .build();

        ConnectCluster connectClusterOwner = ConnectCluster.builder()
                .metadata(ObjectMeta.builder()
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

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        boolean actual = connectClusterService.isNamespaceAllowedForConnectCluster(namespace, "not-allowed-prefix.connect-cluster");

        Assertions.assertFalse(actual);
    }

    /**
     * Test vault password if no connect cluster with aes256 config define.
     */
    @Test
    void vaultPasswordWithoutFormat() {
        String encryptionKey = "changeitchangeitchangeitchangeit";

        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key(EncryptionUtils.encryptAES256GCM("aes256Key", encryptionKey))
                        .aes256Salt(EncryptionUtils.encryptAES256GCM("aes256Salt", encryptionKey))
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        when(securityConfig.getAes256EncryptionKey()).thenReturn("changeitchangeitchangeitchangeit");

        List<VaultResponse> actual = connectClusterService.vaultPassword(namespace, "prefix.connect-cluster", List.of("secret"));

        Assertions.assertTrue(actual.get(0).getSpec().getEncrypted().matches("^\\$\\{aes256\\:.*\\}"));
    }

    /**
     * Test vault password if no connect cluster with aes256 config define and format.
     */
    @Test
    void vaultPasswordWithFormat() {
        String encryptionKey = "changeitchangeitchangeitchangeit";

        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key(EncryptionUtils.encryptAES256GCM("aes256Key", encryptionKey))
                        .aes256Salt(EncryptionUtils.encryptAES256GCM("aes256Salt", encryptionKey))
                        .aes256Format("%s")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
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

        when(securityConfig.getAes256EncryptionKey()).thenReturn("changeitchangeitchangeitchangeit");

        List<VaultResponse> actual = connectClusterService.vaultPassword(namespace, "prefix.connect-cluster", List.of("secret"));

        Assertions.assertFalse(actual.get(0).getSpec().getEncrypted().matches("^\\$\\{aes256\\:.*\\}"));
    }
}
