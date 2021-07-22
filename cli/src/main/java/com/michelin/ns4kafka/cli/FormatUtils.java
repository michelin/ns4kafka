package com.michelin.ns4kafka.cli;

import java.util.List;
import java.util.Optional;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.Status;

import org.ocpsoft.prettytime.PrettyTime;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;


public class FormatUtils {

    private static final String yaml = "yaml";
    private static final String table = "table";

    public static void displayList(ApiResource apiResource, List<Resource> resources, String output) {
        if (output.equals(table)) {
            CommandLine.Help.TextTable tt = CommandLine.Help.TextTable.forColumns(
                    CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO),
                    new CommandLine.Help.Column[]
                            {
                                    new CommandLine.Help.Column(50, 2, CommandLine.Help.Column.Overflow.SPAN),
                                    new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN)
                            });
            tt.addRowValues(apiResource.getKind(), "AGE");
            resources.forEach(resource -> tt.addRowValues(resource.getMetadata().getName(), new PrettyTime().format(resource.getMetadata().getCreationTimestamp())));
            System.out.println(tt);
        } else if (output.equals(yaml)) {
            //TODO
        }
    }

    public static void displayIndividual(Resource resource, String output) {
        if (output.equals(yaml)){
            displayIndividualYaml(resource);
        }
    }

    public static void displayError(HttpClientResponseException e) {

        Optional<Status> statusOptional = e.getResponse().getBody(Status.class);
        if (statusOptional.isPresent()) {
            displayIndividualYaml(statusOptional.get());

        } else {
            System.out.println(e.getMessage());
        }
    }


    private static <T> void displayIndividualYaml(T elementToDisplay) {
        DumperOptions options = new DumperOptions();
        options.setExplicitStart(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Representer representer = new Representer();
        representer.addClassTag(elementToDisplay.getClass(), Tag.MAP);
        System.out.println(new Yaml(representer, options).dump(elementToDisplay));

    }

}
