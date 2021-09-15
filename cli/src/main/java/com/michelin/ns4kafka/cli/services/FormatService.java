package com.michelin.ns4kafka.cli.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.KafkactlConfig;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.Status;
import io.micronaut.core.naming.conventions.StringConvention;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.ocpsoft.prettytime.PrettyTime;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import picocli.CommandLine;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Singleton
public class FormatService {

    @Inject
    public KafkactlConfig kafkactlConfig;

    private static final String YAML = "yaml";
    private static final String TABLE = "table";
    private final List<String> defaults = List.of(
            "KIND:/kind",
            "NAME:/metadata/name",
            "AGE:/metadata/creationTimestamp%AGO"
            );

    public void displayList(String kind, List<Resource> resources, String output) {
        if (output.equals(TABLE)) {
            printTable(kind, resources);
        } else if (output.equals(YAML)) {
            printYaml(resources);
        }
    }

    public void displaySingle(Resource resource, String output) {
        displayList(resource.getKind(), List.of(resource), output);
    }

    public void displayError(HttpClientResponseException e, String kind, String name) {

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

    private void printTable(String kind, List<Resource> resources) {
        String hyphenatedKind = StringConvention.HYPHENATED.format(kind);
        List<String> formats = kafkactlConfig.tableFormat.getOrDefault(hyphenatedKind, defaults);

        PrettyTextTable ptt = new PrettyTextTable(formats, resources);
        System.out.println(ptt);
    }

    private void printYaml(List<Resource> resources) {
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
            CommandLine.Help.Column[] sizedColumns = this.columns
                    .stream()
                    .map(column -> new CommandLine.Help.Column(column.size, 2, CommandLine.Help.Column.Overflow.SPAN))
                    .toArray(CommandLine.Help.Column[]::new);

            CommandLine.Help.TextTable tt = CommandLine.Help.TextTable.forColumns(
                    CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO),
                    sizedColumns
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
                JsonNode cell = node.at(this.jsonPointer);
                switch (this.transform) {
                    case "AGO":
                        try {
                            Date d = Date.from(Instant.parse(cell.asText()));
                            output = new PrettyTime().format(d);
                        } catch (DateTimeParseException e) {
                            output = "err:" + cell;
                        }
                        break;
                    case "PERIOD":
                        try {
                            long ms = Long.parseLong(cell.asText());
                            long days = TimeUnit.MILLISECONDS.toDays(ms);
                            long hours = TimeUnit.MILLISECONDS.toHours(ms - TimeUnit.DAYS.toMillis(days)) ;
                            long minutes = TimeUnit.MILLISECONDS.toMinutes(ms - TimeUnit.DAYS.toMillis(days) - TimeUnit.HOURS.toMillis(hours));
                            output = days > 0 ? (days + "d") : "";
                            output += hours > 0 ? (hours + "h") : "";
                            output += minutes > 0 ? (minutes + "m") : "";
                        } catch (NumberFormatException e){
                            output = "err:" + cell;
                        }
                        break;
                    case "NONE":
                    default:
                        if(cell.isArray()){
                            List<String> childs = new ArrayList<>();
                            cell.elements().forEachRemaining(jsonNode -> childs.add(jsonNode.asText()));
                            output = "["+String.join(",", childs)+"]";
                        } else {
                            output = cell.asText();
                        }
                        break;
                }
                // Check size for later
                size = Math.max(size, output.length() + 2);
                return output;
            }
        }
    }
}
