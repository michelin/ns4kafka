package com.michelin.ns4kafka.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Command(name = "get", description = {
        "Get resources by resource type for the current namespace",
        "Examples:",
        "  kafkactl get topic topic1 : Display topic1 configuration",
        "  kafkactl get topics : Display all topics",
        "  kafkactl get all : Display all resources",
        "Parameters: "
})
public class GetSubcommand implements Callable<Integer> {

    @Inject
    NamespacedResourceClient namespacedClient;
    @Inject
    ClusterResourceClient nonNamespacedClient;

    @Inject
    LoginService loginService;
    @Inject
    ApiResourcesService apiResourcesService;
    @Inject
    KafkactlConfig kafkactlConfig;

    @CommandLine.ParentCommand
    KafkactlCommand kafkactlCommand;
    @Parameters(index = "0", description = "Resource type or 'all' to display resources for all types", arity = "1")
    String resourceType;
    @Parameters(index = "1", description = "Resource name", arity = "0..1")
    Optional<String> resourceName;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    @Override
    public Integer call() throws Exception {

        // 1. Authent
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            return 1;
        }

        // 2. validate resourceType
        List<ApiResource> apiResources = validateResourceType();

        displayAllResoucesForTypesAndName(apiResources);

        return 0;
    }

    private void displayAllResoucesForTypesAndName(List<ApiResource> apiResources) {
        if(resourceName.isEmpty()){
            apiResources.forEach(this::displayAllResourcesForType);
        }else{
            apiResources.forEach(apiResource -> displaySingleResourceWithName(apiResource, resourceName.get()));
        }
    }

    private List<ApiResource> validateResourceType() {
        // specific case ALL
        if (resourceType.equalsIgnoreCase("ALL")) {
            return apiResourcesService.getListResourceDefinition()
                    .stream()
                    .filter(apiResource -> apiResource.isNamespaced())
                    .collect(Collectors.toList());
        }
        // normal usage, check resource exists
        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isPresent()) {
            return List.of(optionalApiResource.get());
        }
        throw new CommandLine.ParameterException(commandSpec.commandLine(), "The server doesn't have resource type " + resourceType);

    }

    private void displaySingleResourceWithName(ApiResource apiResource, String resourceName) {
        Resource resource;
        if(apiResource.isNamespaced()){
            String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());
            resource = namespacedClient.get(namespace, apiResource.getPath(), resourceName, loginService.getAuthorization());

            try{
                ObjectMapper mapper = new ObjectMapper();
                System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(resource));
            } catch (JsonProcessingException e) {
                System.out.println("Error parsing JSON");
                System.out.println(resource.getKind()+"/"+resource.getMetadata().getName());
            }

        }
        //TODO
        // else
    }

    private void displayAllResourcesForType(ApiResource apiResource) {
        List<Resource> resources;
        if(apiResource.isNamespaced()) {
            String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());
            try {
                resources = namespacedClient.list(namespace, apiResource.getPath(), loginService.getAuthorization());
            }catch (HttpClientResponseException e){
                System.out.println("Error during list for resource type "+apiResource.getKind()+": "+e.getMessage());
                resources=List.of();
            }
        }else{
            resources = nonNamespacedClient.list(loginService.getAuthorization(), apiResource.getPath());
        }
        resources.stream().forEach(resource -> System.out.println(resource.getKind()+"/"+resource.getMetadata().getName()));
    }

}
