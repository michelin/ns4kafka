package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.KafkaStream;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.StreamService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Tag(name = "Stream")
@Controller(value = "/api/namespaces/{namespace}/streams")
public class StreamController extends NamespacedResourceController {

    @Inject
    StreamService streamService;

    @Get("/")
    List<KafkaStream> list(String namespace){
        Namespace ns = getNamespace(namespace);
        return streamService.findAllForNamespace(ns);

    }

    @Get("/{stream}")
    Optional<KafkaStream> get(String namespace,String stream){

        Namespace ns = getNamespace(namespace);
        return streamService.findByName(ns, stream);

    }

    @Post("/{?dryrun}")
    HttpResponse<KafkaStream> apply(String namespace,@Body @Valid KafkaStream stream, @QueryValue(defaultValue = "false") boolean dryrun){
        Namespace ns = getNamespace(namespace);

        //Creation of the correct ACLs
        if (!streamService.isNamespaceOwnerOfStream(namespace, stream.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid value " + stream.getMetadata().getName()
                    + " for name: Namespace not OWNER of this stream"), "Stream", stream.getMetadata().getName());
        }
        //Augment the Stream
        stream.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        stream.getMetadata().setCluster(ns.getMetadata().getCluster());
        stream.getMetadata().setNamespace(ns.getMetadata().getName());

        //Creation of the correct ACLs
        Optional<KafkaStream> existingStream = streamService.findByName(ns, stream.getMetadata().getName());
        if (existingStream.isPresent() && existingStream.get().equals(stream)){
            return formatHttpResponse(stream, ApplyStatus.unchanged);
        }

        ApplyStatus status = ApplyStatus.created;
        if (existingStream.isPresent()){
            status = ApplyStatus.changed;
        }

        if (dryrun) {
            return formatHttpResponse(stream, status);
        }

        return formatHttpResponse(streamService.create(stream), status);

    }

    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{stream}{?dryrun}")
    HttpResponse delete(String namespace,String stream, @QueryValue(defaultValue = "false") boolean dryrun){

        Namespace ns = getNamespace(namespace);
        if (!streamService.isNamespaceOwnerOfStream(namespace, stream)) {
            throw new ResourceValidationException(List.of("Invalid value " + stream.getMetadata().getName()
                    + " for name: Namespace not OWNER of this stream"), "Stream", stream.getMetadata().getName());
        }
        // exists ?
        Optional<KafkaStream> optionalStream = streamService.findByName(ns, stream);

        if (optionalStream.isEmpty())
            return HttpResponse.notFound();

        if (dryrun) {
            return HttpResponse.noContent();
        }
        streamService.delete(optionalStream.get());
        return HttpResponse.noContent();
    }
}
