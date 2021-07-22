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

    private FormatUtils() {
        throw new IllegalStateException("Utility class");
    }

    private static final String YAML = "yaml";
    private static final String TABLE = "table";

    public static void displayList(ApiResource apiResource, List<Resource> resources, String output) {
        if (output.equals(TABLE)) {
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
        } else if (output.equals(YAML)) {
            DumperOptions options = defaultDumperOptions();
            Representer representer = defaultRepresenter(Resource.class);
            System.out.println(new Yaml(representer, options).dumpAll(resources.iterator()));
        }
    }

    public static void displayIndividual(Resource resource, String output) {
        if (output.equals(YAML)){
            DumperOptions options = defaultDumperOptions();
            Representer representer = defaultRepresenter(Resource.class);
            System.out.println(new Yaml(representer, options).dump(resource));
        }
    }

    public static void displayError(HttpClientResponseException e, String kind, String name) {

        Optional<Status> statusOptional = e.getResponse().getBody(Status.class);
        if (statusOptional.isPresent()) {
            Status status = statusOptional.get();
            String causes = "";
            if ((status.getDetails() != null) && (!status.getDetails().getCauses().isEmpty())) {
                causes = status.getDetails().getCauses().toString();
            }
            String nameToPrint = "/" + name;
            if (name == null) {
                nameToPrint = "";
            }
            System.out.println(String.format("Failed : %s%s %s ",kind, nameToPrint, status.getMessage())
                               + causes);
        } else {
            System.out.println(e.getMessage());
        }
    }
    private static DumperOptions defaultDumperOptions() {
        DumperOptions options = new DumperOptions();
        options.setExplicitStart(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        return options;
    }
    private static <T> Representer defaultRepresenter(Class<T> classToFormat) {
        Representer representer = new Representer();
        representer.addClassTag(classToFormat, Tag.MAP);
        return representer;
    }
}
