package com.michelin.ns4kafka.controllers;

import io.micronaut.http.annotation.Controller;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Connects")
@Controller(value = "/api/namespaces/{namespace}/connects")
public class ConnectController {
    //TODO validate calls and forward to Connect REST API (sync ???)
}
