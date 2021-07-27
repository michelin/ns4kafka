package com.michelin.ns4kafka.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


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
        // TODO get formats from config.yml
        List<String> formats = List.of(
                "TOPIC:/metadata/name",
                "RETENTION:/spec/configs/retention.ms%PERIOD",
                "AGE:/metadata/creationTimestamp%AGO",
                "CLUSTER:/metadata/cluster",
                "LABELS:/metadata/labels"
        );
        PrettyTextTable ptt = new PrettyTextTable(formats, resources);
        System.out.println(ptt);
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

    public static class PrettyTextTable {
        private final List<PrettyTextTableColumn> columns = new ArrayList<>();
        private final List<String[]> rows = new ArrayList<>();

        public PrettyTextTable(List<String> formats, List<Resource> resources) {
            // 1. Prepare header columns
            formats.forEach(item -> {
                String[] elements = item.split(":");
                if (elements.length != 2) {
                    throw new IllegalStateException("Expected line with format 'NAME:JSONPOINTER[%TRANSFORM]', got " + elements);
                }
                columns.add(new PrettyTextTableColumn(elements));
            });

            // 2. Prepare rows and update column sizes
            ObjectMapper mapper = new ObjectMapper();
            resources.forEach(resource -> {
                JsonNode node = mapper.valueToTree(resource);
                rows.add(columns.stream()
                        .map(column -> column.transform(node))
                        .toArray(String[]::new)
                );
            });
        }

        @Override
        public String toString() {
            CommandLine.Help.Column[] columns = this.columns
                    .stream()
                    .map(column -> new CommandLine.Help.Column(column.size, 2, CommandLine.Help.Column.Overflow.SPAN))
                    .toArray(CommandLine.Help.Column[]::new);

            CommandLine.Help.TextTable tt = CommandLine.Help.TextTable.forColumns(
                    CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO),
                    columns
            );

            // Create Header Row
            tt.addRowValues(this.columns.stream().map(column -> column.header).toArray(String[]::new));
            // Create Data Rows
            this.rows.forEach(tt::addRowValues);

            return tt.toString();
        }

        static class PrettyTextTableColumn {
            private String header;
            private String jsonPointer;
            private String transform;
            private int size = -1;

            public PrettyTextTableColumn(String... elements) {
                this.header = elements[0];
                if (elements[1].contains("%")) {
                    this.jsonPointer = elements[1].split("%")[0];
                    this.transform = elements[1].split("%")[1];
                } else {
                    this.jsonPointer = elements[1];
                    this.transform = "NONE";
                }
                // Size should consider headers
                this.size = Math.max(this.size, this.header.length() + 2);
            }

            public String transform(JsonNode node) {
                String output = null;
                String cell = node.at(this.jsonPointer).asText();
                switch (this.transform) {
                    case "AGO":
                        try {
                            Date d = Date.from(Instant.parse(cell));
                            output = new PrettyTime().format(d);
                        } catch (DateTimeParseException e) {
                            output = "err:" + cell;
                        }
                        break;
                    case "PERIOD":
                        try {
                            long ms = Long.parseLong(cell);
                            long days = TimeUnit.MILLISECONDS.toDays(ms);
                            long hours = TimeUnit.MILLISECONDS.toHours(ms) - (days * 24);
                            long minutes = TimeUnit.MILLISECONDS.toMinutes(ms) - (days * 24) - (hours * 60);
                            output = days > 0 ? (days + "d") : "";
                            output += hours > 0 ? (hours + "h") : "";
                            output += minutes > 0 ? (minutes + "m") : "";
                        } catch (NumberFormatException e){
                            output = "err:" + cell;
                        }
                        break;
                    case "NONE":
                    default:
                        output = cell;
                        break;
                }
                // Check size for later
                size = Math.max(size, output.length() + 2);
                return output;
            }
        }
    }
}
