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

    @Post("/")
    KafkaStream apply(String namespace,@Body KafkaStream stream){
        return null;

    }

    @Delete("/{stream}")
    KafkaStream apply(String namespace,String stream){
        return null;

    }



}
