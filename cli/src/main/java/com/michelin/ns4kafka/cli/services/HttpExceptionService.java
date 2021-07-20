package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.Status;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class HttpExceptionService {

    public void printError(HttpClientResponseException e) {

        Optional<Status> statusOptional = e.getResponse().getBody(Status.class);
        if (statusOptional.isPresent()) {
            displayIndividual(statusOptional.get());

        } else {
            System.out.println(e.getMessage());
        }
    }

    private void displayIndividual(Status status) {
        DumperOptions options = new DumperOptions();
        options.setExplicitStart(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Representer representer = new Representer();
        representer.addClassTag(Resource.class, Tag.MAP);
        System.out.println(new Yaml(representer, options).dump(status));
    }
}
