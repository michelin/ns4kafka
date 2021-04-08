package com.michelin.ns4kafka.cli;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.micronaut.http.HttpMethod;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.simple.SimpleHttpRequest;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "login", description = "Store JSON Web Token return by credentials")
class Login implements Callable<Integer> {

    @Inject
    @Client("http://localhost:8080")
    RxHttpClient client;

    @Option(names = {"-u", "--username"}, description = "Username")
    String username = "Gitlab";

    //TODO change to char[]
    @Option(names = {"-p", "--password"}, description = "Password")
    String password;

    @Override
    public Integer call() throws Exception {
        ObjectMapper mapper  = new ObjectMapper();
        ObjectNode object = mapper.createObjectNode();
        object.put("username", username);
        object.put("password", password);
        String json = mapper.writeValueAsString(object);
        SimpleHttpRequest<String> request = new SimpleHttpRequest<>(HttpMethod.POST, "/login", json);
        //TODO change to async
        String resultJson = client.toBlocking().retrieve(request);
        JsonNode resultObject = mapper.readTree(resultJson);
        System.out.println(resultObject.get("access_token"));
        File file = new File("jwt");
        file.createNewFile();
        FileWriter myWriter = new FileWriter("jwt");
        myWriter.write(resultObject.get("access_token").textValue());
        myWriter.close();
        return 0;
    }
}
