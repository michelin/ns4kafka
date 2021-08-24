package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.Status;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.ocpsoft.prettytime.PrettyTime;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import picocli.CommandLine;

import java.util.List;
import java.util.Optional;


public class FormatUtils {

    private FormatUtils() {
        throw new IllegalStateException("Utility class");
    }

    private static final String YAML = "yaml";
    private static final String TABLE = "table";

    public static void displayList(ApiResource apiResource, List<Resource> resources, String output) {
        if (output.equals(TABLE)) {
            printTable(apiResource, resources);
        } else if (output.equals(YAML)) {
            printYaml(resources);
        }
    }
    public static void displayLogs(List<LogSubcommand.AuditLog> auditLogs) {
        //TODO optimize with dynamic columns
        CommandLine.Help.TextTable tt = CommandLine.Help.TextTable.forColumns(
                CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO),
                new CommandLine.Help.Column(50, 2, CommandLine.Help.Column.Overflow.SPAN),
                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN),
                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN),
                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN),
                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN));
        tt.addRowValues("USER", "DATE", "OPERATION", "RESOURCE_KIND", "RESOURCE_NAME");


        auditLogs.forEach(auditLog -> tt.addRowValues(
                auditLog.getUser().getUsername(),
                auditLog.getDate(),
                auditLog.getOperation(),
                auditLog.getKind(),
                auditLog.getMetadata().getName()));
        System.out.println(tt);

    }

    public static void displaySingle(ApiResource apiResource, Resource resource, String output) {
        displayList(apiResource, List.of(resource), output);
    }

    public static void displayError(HttpClientResponseException e, String kind, String name) {

        Optional<Status> statusOptional = e.getResponse().getBody(Status.class);
        if (statusOptional.isPresent()) {
            Status status = statusOptional.get();
            String causes = "";
            if ((status.getDetails() != null) && (!status.getDetails().getCauses().isEmpty())) {
                causes = status.getDetails().getCauses().toString();
            }

            System.out.printf("Failed : %s/%s %s %s%n", kind, name, status.getMessage(), causes);
        } else {
            System.out.printf("Failed : %s/%s %s%n", kind, name, e.getMessage());
        }
    }

    private static void printTable(ApiResource apiResource, List<Resource> resources) {
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
    }

    private static void printYaml(List<Resource> resources) {
        DumperOptions options = new DumperOptions();
        options.setExplicitStart(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Representer representer = new Representer();
        representer.addClassTag(Resource.class, Tag.MAP);
        Yaml yaml = new Yaml(representer, options);
        System.out.println(yaml.dumpAll(resources.iterator()));
    }

}
