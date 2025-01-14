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

package com.michelin.ns4kafka.service.client.schema;

import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityRequest;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaRequest;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaResponse;
import com.michelin.ns4kafka.service.client.schema.entities.TagInfo;
import com.michelin.ns4kafka.service.client.schema.entities.TagTopicInfo;
import com.michelin.ns4kafka.service.client.schema.entities.TopicDescriptionUpdateBody;
import com.michelin.ns4kafka.service.client.schema.entities.TopicDescriptionUpdateResponse;
import com.michelin.ns4kafka.service.client.schema.entities.TopicListResponse;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Schema registry client.
 */
@Slf4j
@Singleton
public class SchemaRegistryClient {
    private static final String SUBJECTS = "/subjects/";
    private static final String CONFIG = "/config/";
    private static final String VERSIONS = "/versions/";

    @Inject
    @Client(id = "schema-registry")
    private HttpClient httpClient;

    @Inject
    private List<ManagedClusterProperties> managedClusterProperties;

    /**
     * List subjects.
     *
     * @param kafkaCluster The Kafka cluster
     * @return A list of subjects
     */
    public Flux<String> getSubjects(String kafkaCluster) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects")))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Flux.from(httpClient.retrieve(request, String[].class)).flatMap(Flux::fromArray);
    }

    /**
     * Get a subject by it name and id.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @param version      The subject version
     * @return A subject
     */
    public Mono<SchemaResponse> getSubject(String kafkaCluster, String subject, String version) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);
        String encodedVersion = URLEncoder.encode(version, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.GET(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    SUBJECTS + encodedSubject + VERSIONS + encodedVersion)))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, SchemaResponse.class))
            .onErrorResume(HttpClientResponseException.class,
                ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Mono.empty() : Mono.error(ex));
    }

    /**
     * Get all the versions of a given subject.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @return All the versions of a subject
     */
    public Flux<SchemaResponse> getAllSubjectVersions(String kafkaCluster, String subject) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.GET(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    SUBJECTS + encodedSubject + "/versions")))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Flux.from(httpClient.retrieve(request, Integer[].class))
            .flatMap(ids -> Flux.fromIterable(Arrays.asList(ids))
                .flatMap(id -> {
                    HttpRequest<?> requestVersion = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(),
                            SUBJECTS + encodedSubject + VERSIONS + id)))
                        .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

                    return httpClient.retrieve(requestVersion, SchemaResponse.class);
                }))
            .onErrorResume(HttpClientResponseException.class,
                ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Flux.empty() : Flux.error(ex));
    }

    /**
     * Register a subject and a schema.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @param body         The schema
     * @return The response of the registration
     */
    public Mono<SchemaResponse> register(String kafkaCluster, String subject, SchemaRequest body) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.POST(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    SUBJECTS + encodedSubject + "/versions")), body)
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, SchemaResponse.class));
    }

    /**
     * Delete a subject.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @param hardDelete   Should the subject be hard deleted or not
     * @return The versions of the deleted subject
     */
    public Mono<Integer[]> deleteSubject(String kafkaCluster, String subject, boolean hardDelete) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);

        MutableHttpRequest<?> request = HttpRequest.DELETE(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    SUBJECTS + encodedSubject + "?permanent=" + hardDelete)))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, Integer[].class));
    }

    /**
     * Delete schema version under a subject.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @param version      The version
     * @param hardDelete   Should the subject be hard deleted or not
     * @return The version of the deleted subject
     */
    public Mono<Integer> deleteSubjectVersion(String kafkaCluster, String subject, String version, boolean hardDelete) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);
        String encodedVersion = URLEncoder.encode(version, StandardCharsets.UTF_8);

        MutableHttpRequest<?> request = HttpRequest.DELETE(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    SUBJECTS + encodedSubject + VERSIONS + encodedVersion + "?permanent=" + hardDelete)))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, Integer.class));
    }

    /**
     * Validate the schema compatibility.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @param body         The request
     * @return The schema compatibility validation
     */
    public Mono<SchemaCompatibilityCheckResponse> validateSchemaCompatibility(String kafkaCluster,
                                                                              String subject,
                                                                              SchemaRequest body) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.POST(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    "/compatibility/subjects/" + encodedSubject + "/versions?verbose=true")), body)
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityCheckResponse.class))
            .onErrorResume(HttpClientResponseException.class,
                ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Mono.empty() : Mono.error(ex));
    }

    /**
     * Update the subject compatibility.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @param body         The schema compatibility request
     * @return The schema compatibility update
     */
    public Mono<SchemaCompatibilityResponse> updateSubjectCompatibility(String kafkaCluster,
                                                                        String subject,
                                                                        SchemaCompatibilityRequest body) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.PUT(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    CONFIG + encodedSubject)), body)
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class));
    }

    /**
     * Get the current compatibility by subject.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @return The current schema compatibility
     */
    public Mono<SchemaCompatibilityResponse> getCurrentCompatibilityBySubject(String kafkaCluster, String subject) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.GET(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    CONFIG + encodedSubject)))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class))
            .onErrorResume(HttpClientResponseException.class,
                ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Mono.empty() : Mono.error(ex));
    }

    /**
     * Delete current compatibility by subject.
     *
     * @param kafkaCluster The Kafka cluster
     * @param subject      The subject
     * @return The deleted schema compatibility
     */
    public Mono<SchemaCompatibilityResponse> deleteCurrentCompatibilityBySubject(String kafkaCluster, String subject) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8);

        MutableHttpRequest<?> request = HttpRequest.DELETE(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    CONFIG + encodedSubject)))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class));
    }

    /**
     * Add a tag to a topic.
     *
     * @param kafkaCluster The Kafka cluster
     * @param tagSpecs     Tags to add
     * @return Information about added tags
     */
    public Mono<List<TagTopicInfo>> associateTags(String kafkaCluster, List<TagTopicInfo> tagSpecs) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);

        HttpRequest<?> request = HttpRequest.POST(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    "/catalog/v1/entity/tags")), tagSpecs)
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, Argument.listOf(TagTopicInfo.class)));
    }

    /**
     * Create tags.
     *
     * @param tags         The list of tags to create
     * @param kafkaCluster The Kafka cluster
     * @return Information about created tags
     */
    public Mono<List<TagInfo>> createTags(String kafkaCluster, List<TagInfo> tags) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);

        HttpRequest<?> request = HttpRequest.POST(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    "/catalog/v1/types/tagdefs")), tags)
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, Argument.listOf(TagInfo.class)));
    }

    /**
     * Delete a tag to a topic.
     *
     * @param kafkaCluster The Kafka cluster
     * @param entityName   The topic's name
     * @param tagName      The tag to delete
     * @return The resume response
     */
    public Mono<HttpResponse<Void>> dissociateTag(String kafkaCluster, String entityName, String tagName) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);

        HttpRequest<?> request = HttpRequest.DELETE(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    "/catalog/v1/entity/type/kafka_topic/name/" + entityName + "/tags/" + tagName)))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * List topics with catalog info, including tag & description.
     *
     * @param kafkaCluster The Kafka cluster
     * @return A list of description
     */
    public Mono<TopicListResponse> getTopicWithCatalogInfo(String kafkaCluster, int limit, int offset) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);

        HttpRequest<?> request = HttpRequest.GET(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    "/catalog/v1/search/basic?type=kafka_topic&limit=" + limit + "&offset=" + offset)))
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, TopicListResponse.class));
    }

    /**
     * Update a topic description.
     *
     * @param kafkaCluster The Kafka cluster
     * @param body         The body passed to the request
     * @return Information about description
     */
    public Mono<HttpResponse<TopicDescriptionUpdateResponse>> updateDescription(String kafkaCluster,
                                                                                TopicDescriptionUpdateBody body) {
        ManagedClusterProperties.SchemaRegistryProperties config = getSchemaRegistry(kafkaCluster);

        HttpRequest<?> request = HttpRequest.PUT(
                URI.create(StringUtils.prependUri(config.getUrl(),
                    "/catalog/v1/entity")), body)
            .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.exchange(request, TopicDescriptionUpdateResponse.class));
    }

    /**
     * Get the schema registry of the given Kafka cluster.
     *
     * @param kafkaCluster The Kafka cluster
     * @return The schema registry configuration
     */
    private ManagedClusterProperties.SchemaRegistryProperties getSchemaRegistry(String kafkaCluster) {
        Optional<ManagedClusterProperties> config = managedClusterProperties.stream()
            .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
            .findFirst();

        if (config.isEmpty()) {
            throw new ResourceValidationException(null, null,
                List.of("Kafka Cluster [" + kafkaCluster + "] not found"));
        }

        if (config.get().getSchemaRegistry() == null) {
            throw new ResourceValidationException(null, null,
                List.of("Kafka Cluster [" + kafkaCluster + "] has no schema registry"));
        }

        return config.get().getSchemaRegistry();
    }
}
