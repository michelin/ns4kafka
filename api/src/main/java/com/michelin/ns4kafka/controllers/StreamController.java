package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.KafkaStream;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.services.StreamService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
    HttpResponse<KafkaStream> apply(String namespace,@Body KafkaStream stream, @QueryValue(defaultValue = "false") boolean dryrun){
        Namespace ns = getNamespace(namespace);

        //Creation of the correct ACLs
        if (!streamService.isNamespaceOwnerOfStream(namespace, stream.getMetadata().getName())) {
            //TODO throw error
        }
        //Augment the Stream
        stream.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        stream.getMetadata().setCluster(ns.getMetadata().getCluster());
        stream.getMetadata().setNamespace(ns.getMetadata().getName());

        //Creation of the correct ACLs
        ApplyStatus status = ApplyStatus.created;

        return formatHttpResponse(stream, status);

    }

    @Delete("/{stream}{?dryrun}")
    HttpResponse delete(String namespace,String stream, @QueryValue(defaultValue = "false") boolean dryrun){
        return null;

    }



}
