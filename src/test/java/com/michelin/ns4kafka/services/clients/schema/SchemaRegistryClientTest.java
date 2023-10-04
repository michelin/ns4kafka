package com.michelin.ns4kafka.services.clients.schema;

import static com.michelin.ns4kafka.services.executors.TopicAsyncExecutor.TOPIC_ENTITY_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.services.clients.schema.entities.TagInfo;
import com.michelin.ns4kafka.services.clients.schema.entities.TagSpecs;
import com.michelin.ns4kafka.services.clients.schema.entities.TagTopicInfo;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

/**
 * Test schema registry.
 */
@ExtendWith(MockitoExtension.class)
public class SchemaRegistryClientTest {

    private static final String KAFKA_CLUSTER = "local";
    private static final String TAG = "TAG";
    private static final String ENTITY_NAME = "ENTITY_NAME";

    @Mock
    HttpClient httpClient;

    @Mock
    List<ManagedClusterProperties> managedClusterPropertiesList;

    @InjectMocks
    SchemaRegistryClient schemaRegistryClient;

    static ManagedClusterProperties.SchemaRegistryProperties schemaRegistryProperties;

    static ManagedClusterProperties managedClusterProperties;

    @BeforeAll
    static void init() {
        schemaRegistryProperties = new ManagedClusterProperties.SchemaRegistryProperties();
        schemaRegistryProperties.setUrl("URL");
        schemaRegistryProperties.setBasicAuthUsername("USER");
        schemaRegistryProperties.setBasicAuthPassword("PASSWORD");

        managedClusterProperties =
                new ManagedClusterProperties(KAFKA_CLUSTER, ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        managedClusterProperties.setSchemaRegistry(schemaRegistryProperties);
    }

    @Test
    void getTagsTest() {
        TagInfo tagInfo = TagInfo.builder().name(TAG).build();

        when(managedClusterPropertiesList.stream()).thenReturn(
                Stream.of(managedClusterProperties));
        when(httpClient.retrieve((HttpRequest<Object>) any(), (Argument<Object>) any()))
                .thenReturn(Mono.just(List.of(tagInfo)));

        Mono<List<TagInfo>> tagInfos = schemaRegistryClient.getTags(KAFKA_CLUSTER);

        assertEquals(TAG, tagInfos.block().get(0).name());
    }

    @Test
    void getTopicWithTagsTest() {
        TagTopicInfo tagTopicInfo = TagTopicInfo.builder()
                .entityType(TOPIC_ENTITY_TYPE)
                .typeName(TAG)
                .entityName(ENTITY_NAME).build();

        when(managedClusterPropertiesList.stream()).thenReturn(
                Stream.of(managedClusterProperties));
        when(httpClient.retrieve((HttpRequest<Object>) any(), (Argument<Object>) any()))
                .thenReturn(Mono.just(List.of(tagTopicInfo)));

        Mono<List<TagTopicInfo>> tagInfos = schemaRegistryClient.getTopicWithTags(KAFKA_CLUSTER, ENTITY_NAME);

        assertEquals(ENTITY_NAME, tagInfos.block().get(0).entityName());
        assertEquals(TAG, tagInfos.block().get(0).typeName());
        assertEquals(TOPIC_ENTITY_TYPE, tagInfos.block().get(0).entityType());
    }

    @Test
    void addTagsTest() {
        TagTopicInfo tagTopicInfo = TagTopicInfo.builder()
                .entityType(TOPIC_ENTITY_TYPE)
                .entityName(ENTITY_NAME)
                .typeName(TAG).build();

        TagSpecs tagSpecs = TagSpecs.builder()
                .entityType(TOPIC_ENTITY_TYPE)
                .entityName(ENTITY_NAME)
                .typeName(TAG).build();

        when(managedClusterPropertiesList.stream()).thenReturn(
                Stream.of(managedClusterProperties));
        when(httpClient.retrieve((HttpRequest<Object>) any(), (Argument<Object>) any()))
                .thenReturn(Mono.just(List.of(tagTopicInfo)));

        Mono<List<TagTopicInfo>> tagInfos = schemaRegistryClient.addTags(KAFKA_CLUSTER, List.of(tagSpecs));

        assertEquals(ENTITY_NAME, tagInfos.block().get(0).entityName());
        assertEquals(TAG, tagInfos.block().get(0).typeName());
        assertEquals(TOPIC_ENTITY_TYPE, tagInfos.block().get(0).entityType());
    }

    @Test
    void deleteTagTest() {
        when(managedClusterPropertiesList.stream()).thenReturn(
                Stream.of(managedClusterProperties));
        when(httpClient.exchange((HttpRequest<Object>) any(), (Class<Object>) any()))
                .thenReturn(Mono.just(HttpResponse.accepted()));

        Mono<HttpResponse<Void>> deleteInfo = schemaRegistryClient.deleteTag(KAFKA_CLUSTER, ENTITY_NAME, TAG);

        assertNotNull(deleteInfo);
    }
}
