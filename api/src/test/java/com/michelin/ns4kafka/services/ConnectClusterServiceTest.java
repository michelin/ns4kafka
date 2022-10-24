package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ConnectCluster;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.ConnectClusterRepository;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientException;
import io.reactivex.Flowable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConnectClusterServiceTest {
    @Mock
    ConnectClusterRepository connectClusterRepository;

    @Mock
    AccessControlEntryService accessControlEntryService;

    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;

    @InjectMocks
    ConnectClusterService connectClusterService;

    @Mock
    @Client("/")
    RxHttpClient httpClient;

    /**
     * Test find all
     */
    @Test
    void findAllEmpty() {
        Mockito.when(connectClusterRepository.findAll()).thenReturn(List.of());
        List<ConnectCluster> actual = connectClusterRepository.findAll();

        Assertions.assertTrue(actual.isEmpty());
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

        Mockito.when(connectClusterRepository.findAll()).thenReturn(List.of(connectCluster));
        List<ConnectCluster> actual = connectClusterService.findAll();

        Assertions.assertEquals(1L, actual.size());
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
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("prefix.")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("prefix2.connect-two")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.READ)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
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

        List<ConnectCluster> actual = connectClusterService.findAllForNamespace(namespace);

        Assertions.assertEquals(2, actual.size());
        // contains
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix.connect-cluster")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix2.connect-two")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("not-owner")));
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("prefix3.connect-cluster")));
    }

    /**
     * Test find by name
     */
    @Test
    void findByName() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        Mockito.when(connectClusterRepository.findAll()).thenReturn(List.of(connectCluster));
        Optional<ConnectCluster> actual = connectClusterService.findByName("prefix.connect-cluster");

        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("prefix.connect-cluster", actual.get().getMetadata().getName());
    }

    /**
     * Test find by name empty response
     */
    @Test
    void findByNameEmpty() {
        Mockito.when(connectClusterRepository.findAll()).thenReturn(List.of());
        Optional<ConnectCluster> actual = connectClusterService.findByName("prefix.connect-cluster");

        Assertions.assertTrue(actual.isEmpty());
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
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("prefix.")
                                        .build())
                                .build()
                ));

        Optional<ConnectCluster> actual = connectClusterService.findByNamespaceAndName(namespace, "prefix.connect-cluster");

        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("prefix.connect-cluster", actual.get().getMetadata().getName());
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
                .metadata(ObjectMeta.builder().name("prefix.connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        when(connectClusterRepository.findAllForCluster("local"))
                .thenReturn(List.of(connectCluster));

        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("prefix.")
                                        .build())
                                .build()
                ));

        Optional<ConnectCluster> actual = connectClusterService.findByNamespaceAndName(namespace, "does-not-exist");

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
        Assertions.assertEquals(actual, connectCluster);
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
        when(httpClient.exchange(any(MutableHttpRequest.class))).thenReturn(Flowable.just(HttpResponse.ok()));

        List<String> errors = connectClusterService.validateConnectClusterCreation(connectCluster);

        Assertions.assertEquals(1L, errors.size());
        Assertions.assertEquals("A Connect cluster is already defined globally with the name test-connect. Please provide a different name.", errors.get(0));
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
        when(httpClient.exchange(any(MutableHttpRequest.class))).thenReturn(Flowable.just(HttpResponse.serverError()));

        List<String> errors = connectClusterService.validateConnectClusterCreation(connectCluster);

        Assertions.assertEquals(1L, errors.size());
        Assertions.assertEquals("The Connect cluster test-connect is not healthy (HTTP code 500).", errors.get(0));
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

        List<String> errors = connectClusterService.validateConnectClusterCreation(connectCluster);

        Assertions.assertEquals(1L, errors.size());
        Assertions.assertEquals("The Connect cluster test-connect has a malformed URL \"malformed-url\".", errors.get(0));
    }

    /**
     * Test validate connect cluster creation throws http client exception
     */
    @Test
    void validateConnectClusterCreationHttpClientException() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("test-connect")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of());
        when(httpClient.exchange(any(MutableHttpRequest.class)))
                .thenThrow(new HttpClientException("Error"));

        List<String> errors = connectClusterService.validateConnectClusterCreation(connectCluster);

        Assertions.assertEquals(1L, errors.size());
        Assertions.assertEquals("The following error occurred trying to check the Connect cluster test-connect health: Error.", errors.get(0));
    }
}
