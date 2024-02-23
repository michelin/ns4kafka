package com.michelin.ns4kafka.controllers;

import static com.michelin.ns4kafka.utils.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.utils.enums.Kind.KAFKA_STREAM;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.KafkaStream;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.StreamService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
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

/**
 * Controller to manage Kafka Streams.
 */
@Tag(name = "Kafka Streams", description = "Manage the Kafka Streams.")
@Controller(value = "/api/namespaces/{namespace}/streams")
public class StreamController extends NamespacedResourceController {
    @Inject
    StreamService streamService;

    /**
     * List Kafka Streams by namespace.
     *
     * @param namespace The namespace
     * @return A list of Kafka Streams
     */
    @Get("/")
    List<KafkaStream> list(String namespace) {
        return streamService.findAllForNamespace(getNamespace(namespace));
    }

    /**
     * Get a Kafka Streams by namespace and name.
     *
     * @param namespace The name
     * @param stream    The Kafka Streams name
     * @return The Kafka Streams
     */
    @Get("/{stream}")
    Optional<KafkaStream> get(String namespace, String stream) {
        return streamService.findByName(getNamespace(namespace), stream);
    }

    /**
     * Create a Kafka Streams.
     *
     * @param namespace The namespace
     * @param stream    The Kafka Stream
     * @param dryrun    Is dry run mode or not ?
     * @return An HTTP response
     */
    @Post("/{?dryrun}")
    HttpResponse<KafkaStream> apply(String namespace, @Body @Valid KafkaStream stream,
                                    @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);
        if (!streamService.isNamespaceOwnerOfKafkaStream(ns, stream.getMetadata().getName())) {
            throw new ResourceValidationException(stream, invalidOwner(stream.getMetadata().getName()));
        }

        stream.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        stream.getMetadata().setCluster(ns.getMetadata().getCluster());
        stream.getMetadata().setNamespace(ns.getMetadata().getName());

        // Creation of the correct ACLs
        Optional<KafkaStream> existingStream = streamService.findByName(ns, stream.getMetadata().getName());
        if (existingStream.isPresent() && existingStream.get().equals(stream)) {
            return formatHttpResponse(stream, ApplyStatus.unchanged);
        }

        ApplyStatus status = existingStream.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        if (dryrun) {
            return formatHttpResponse(stream, status);
        }

        sendEventLog(stream, status, existingStream.<Object>map(KafkaStream::getMetadata).orElse(null),
            stream.getMetadata());

        return formatHttpResponse(streamService.create(stream), status);
    }

    /**
     * Delete a Kafka Streams.
     *
     * @param namespace The namespace
     * @param stream    The Kafka Streams
     * @param dryrun    Is dry run mode or not ?
     * @return An HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{stream}{?dryrun}")
    HttpResponse<Void> delete(String namespace, String stream, @QueryValue(defaultValue = "false") boolean dryrun) {
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
        sendEventLog(streamToDelete, ApplyStatus.deleted, streamToDelete.getMetadata(), null);
        streamService.delete(ns, optionalStream.get());
        return HttpResponse.noContent();
    }
}
