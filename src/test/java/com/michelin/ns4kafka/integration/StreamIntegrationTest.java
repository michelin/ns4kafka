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
package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.integration.TopicIntegrationTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.integration.container.KafkaIntegrationTest;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.model.AccessControlEntry.Permission;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.service.executor.AccessControlEntryAsyncExecutor;
import com.michelin.ns4kafka.service.executor.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
class StreamIntegrationTest extends KafkaIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutorList;

    @Inject
    List<AccessControlEntryAsyncExecutor> aceAsyncExecutorList;

    private String token;

    @BeforeAll
    void init() {
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("nskafkastream")
                        .cluster("test-cluster")
                        .build())
                .spec(NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        AccessControlEntry acl1 = AccessControlEntry.builder()
                .metadata(Metadata.builder()
                        .name("nskafkastream-acl-topic")
                        .namespace("nskafkastream")
                        .build())
                .spec(AccessControlEntrySpec.builder()
                        .resourceType(ResourceType.TOPIC)
                        .resource("kstream-")
                        .resourcePatternType(ResourcePatternType.PREFIXED)
                        .permission(Permission.OWNER)
                        .grantedTo("nskafkastream")
                        .build())
                .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        token = response.getBody().get().getAccessToken();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(namespace));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/nskafkastream/acls")
                        .bearerAuth(token)
                        .body(acl1));

        AccessControlEntry acl2 = AccessControlEntry.builder()
                .metadata(Metadata.builder()
                        .name("nskafkastream-acl-group")
                        .namespace("nskafkastream")
                        .build())
                .spec(AccessControlEntrySpec.builder()
                        .resourceType(ResourceType.GROUP)
                        .resource("kstream-")
                        .resourcePatternType(ResourcePatternType.PREFIXED)
                        .permission(Permission.OWNER)
                        .grantedTo("nskafkastream")
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/nskafkastream/acls")
                        .bearerAuth(token)
                        .body(acl2));
    }

    @Test
    void shouldVerifyCreationOfAcls() throws InterruptedException, ExecutionException {
        KafkaStream stream = KafkaStream.builder()
                .metadata(Metadata.builder().name("kstream-test").build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/nskafkastream/streams")
                        .bearerAuth(token)
                        .body(stream));

        // Force ACL Sync
        aceAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);
        Admin kafkaClient = getAdminClient();

        var aclTopic = kafkaClient
                .describeAcls(new AclBindingFilter(
                        new ResourcePatternFilter(
                                org.apache.kafka.common.resource.ResourceType.TOPIC,
                                stream.getMetadata().getName(),
                                PatternType.PREFIXED),
                        AccessControlEntryFilter.ANY))
                .values()
                .get();

        assertEquals(2, aclTopic.size());
        assertTrue(aclTopic.stream().allMatch(aclBinding -> List.of(AclOperation.CREATE, AclOperation.DELETE)
                .contains(aclBinding.entry().operation())));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.DELETE, "/api/namespaces/nskafkastream/streams?name=kstream*")
                        .bearerAuth(token));

        HttpResponse<List<KafkaStream>> streams = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.GET, "/api/namespaces/nskafkastream/streams")
                                .bearerAuth(token),
                        Argument.listOf(KafkaStream.class));

        assertTrue(streams.getBody().isPresent());
        assertEquals(0, streams.getBody().get().size());
    }

    @Test
    void shouldImportAndDeleteChangelogAndRepartitionTopics()
            throws InterruptedException, ExecutionException, TimeoutException {
        NewTopic changelogTopic = new NewTopic("kstream-appId-MY_TOPIC-changelog", 1, (short) 1);
        NewTopic repartitionTopic = new NewTopic("kstream-appId-MY_TOPIC-repartition", 1, (short) 1);
        NewTopic outsideNs4KafkaTopic = new NewTopic("kstream-MY_TOPIC_CREATED_OUTSIDE_NS4KAFKA", 1, (short) 1);
        NewTopic notCoveredByNamespaceTopic = new NewTopic("kstream2-MY_TOPIC-changelog", 1, (short) 1);

        List<NewTopic> topics =
                List.of(changelogTopic, repartitionTopic, outsideNs4KafkaTopic, notCoveredByNamespaceTopic);

        Admin kafkaClient = getAdminClient();
        kafkaClient.createTopics(topics).all().get(60, TimeUnit.SECONDS);

        forceTopicSynchronization();

        List<Topic> response = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(HttpMethod.GET, "/api/namespaces/nskafkastream/topics")
                                .bearerAuth(token),
                        Argument.listOf(Topic.class));

        assertTrue(response.stream()
                .anyMatch(topic -> topic.getMetadata().getName().equals("kstream-appId-MY_TOPIC-changelog")));
        assertTrue(response.stream()
                .anyMatch(topic -> topic.getMetadata().getName().equals("kstream-appId-MY_TOPIC-repartition")));
        assertTrue(response.stream()
                .noneMatch(topic -> topic.getMetadata().getName().equals("kstream-MY_TOPIC_CREATED_OUTSIDE_NS4KAFKA")));
        assertTrue(response.stream()
                .noneMatch(topic -> topic.getMetadata().getName().equals("kstream2-MY_TOPIC-changelog")));

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("kstream-appId")
                        .namespace("nskafkastream")
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/nskafkastream/streams")
                        .bearerAuth(token)
                        .body(kafkaStream));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.DELETE, "/api/namespaces/nskafkastream/streams")
                        .bearerAuth(token)
                        .body(kafkaStream));

        response = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(HttpMethod.GET, "/api/namespaces/nskafkastream/topics")
                                .bearerAuth(token),
                        Argument.listOf(Topic.class));

        assertTrue(response.stream()
                .noneMatch(topic -> topic.getMetadata().getName().equals("kstream-appId-MY_TOPIC-changelog")));
        assertTrue(response.stream()
                .noneMatch(topic -> topic.getMetadata().getName().equals("kstream-appId-MY_TOPIC-repartition")));
    }

    private void forceTopicSynchronization() throws InterruptedException {
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        // Wait for topics to be updated in Kafka broker
        Thread.sleep(2000);
    }
}
