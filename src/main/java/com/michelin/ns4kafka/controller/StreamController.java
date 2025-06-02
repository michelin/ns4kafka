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
package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.util.enumation.Kind.KAFKA_STREAM;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Status;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Controller to manage Kafka Streams. */
@Tag(name = "Kafka Streams", description = "Manage the Kafka Streams.")
@Controller(value = "/api/namespaces/{namespace}/streams")
public class StreamController extends NamespacedResourceController {
    @Inject
    private StreamService streamService;

    /**
     * List Kafka Streams by namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @return A list of Kafka Streams
     */
    @Get
    List<KafkaStream> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        return streamService.findByWildcardName(getNamespace(namespace), name);
    }

    /**
     * Get a Kafka Streams by namespace and name.
     *
     * @param namespace The name
     * @param stream The Kafka Streams name
     * @return The Kafka Streams
     * @deprecated Use ${@link #list(String, String)}
     */
    @Get("/{stream}")
    @Deprecated(since = "1.12.0")
    Optional<KafkaStream> get(String namespace, String stream) {
        return streamService.findByName(getNamespace(namespace), stream);
    }

    /**
     * Create a Kafka Streams.
     *
     * @param namespace The namespace
     * @param stream The Kafka Stream
     * @param dryrun Is dry run mode or not?
     * @return An HTTP response
     */
    @Post("/{?dryrun}")
    HttpResponse<KafkaStream> apply(
            String namespace, @Body @Valid KafkaStream stream, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);
        if (!streamService.isNamespaceOwnerOfKafkaStream(
                ns, stream.getMetadata().getName())) {
            throw new ResourceValidationException(
                    stream, invalidOwner(stream.getMetadata().getName()));
        }

        stream.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        stream.getMetadata().setCluster(ns.getMetadata().getCluster());
        stream.getMetadata().setNamespace(ns.getMetadata().getName());

        // Creation of the correct ACLs
        Optional<KafkaStream> existingStream =
                streamService.findByName(ns, stream.getMetadata().getName());
        if (existingStream.isPresent() && existingStream.get().equals(stream)) {
            return formatHttpResponse(stream, ApplyStatus.UNCHANGED);
        }

        ApplyStatus status = existingStream.isPresent() ? ApplyStatus.CHANGED : ApplyStatus.CREATED;

        if (dryrun) {
            return formatHttpResponse(stream, status);
        }

        sendEventLog(
                stream,
                status,
                existingStream.<Object>map(KafkaStream::getMetadata).orElse(null),
                stream.getMetadata(),
                EMPTY_STRING);

        return formatHttpResponse(streamService.create(stream), status);
    }

    /**
     * Delete a Kafka Streams.
     *
     * @param namespace The namespace
     * @param stream The Kafka Streams
     * @param dryrun Is dry run mode or not?
     * @return An HTTP response
     * @deprecated use {@link #bulkDelete(String, String, boolean)} instead.
     */
    @Delete("/{stream}{?dryrun}")
    @Deprecated(since = "1.13.0")
    @Status(HttpStatus.NO_CONTENT)
    HttpResponse<Void> delete(String namespace, String stream, @QueryValue(defaultValue = "false") boolean dryrun)
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = getNamespace(namespace);
        if (!streamService.isNamespaceOwnerOfKafkaStream(ns, stream)) {
            throw new ResourceValidationException(KAFKA_STREAM, stream, invalidOwner(stream));
        }

        Optional<KafkaStream> optionalStream = streamService.findByName(ns, stream);

        if (optionalStream.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        var streamToDelete = optionalStream.get();

        sendEventLog(streamToDelete, ApplyStatus.DELETED, streamToDelete.getMetadata(), null, EMPTY_STRING);

        streamService.delete(ns, optionalStream.get());
        return HttpResponse.noContent();
    }

    /**
     * Delete a Kafka Streams.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @param dryrun Is dry run mode or not?
     * @return An HTTP response
     */
    @Delete
    @Status(HttpStatus.OK)
    HttpResponse<List<KafkaStream>> bulkDelete(
            String namespace,
            @QueryValue(defaultValue = "*") String name,
            @QueryValue(defaultValue = "false") boolean dryrun)
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = getNamespace(namespace);
        List<KafkaStream> kafkaStreams = streamService.findByWildcardName(ns, name);

        List<String> validationErrors = kafkaStreams.stream()
                .filter(kafkaStream -> !streamService.isNamespaceOwnerOfKafkaStream(
                        ns, kafkaStream.getMetadata().getName()))
                .map(kafkaStream -> invalidOwner(kafkaStream.getMetadata().getName()))
                .toList();

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(KAFKA_STREAM, name, validationErrors);
        }

        if (kafkaStreams.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.ok(kafkaStreams);
        }

        for (KafkaStream kafkaStream : kafkaStreams) {
            sendEventLog(kafkaStream, ApplyStatus.DELETED, kafkaStream.getMetadata(), null, EMPTY_STRING);
            streamService.delete(ns, kafkaStream);
        }

        return HttpResponse.ok(kafkaStreams);
    }
}
