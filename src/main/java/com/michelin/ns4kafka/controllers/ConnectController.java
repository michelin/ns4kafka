package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.repositories.ConnectRepository;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import java.util.Map;

@Tag(name = "Connects")
@Controller(value = "/api/namespaces/{namespace}/connects")
public class ConnectController {
    //TODO validate calls and forward to Connect REST API (sync ???)
    @Inject
    ConnectRepository connectRepository;


    @Get()
    public Maybe<Map<String, ConnectRepository.ConnectItem>> list(){
        return connectRepository.list("test");
    }


}
