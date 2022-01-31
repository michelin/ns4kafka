package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ConfigService;
import com.michelin.ns4kafka.cli.services.FormatService;
import lombok.Getter;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import picocli.CommandLine;

import javax.inject.Inject;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "config", description = "Manage configuration")
public class ConfigSubcommand implements Callable<Integer> {
    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public ConfigService configService;

    @Inject
    public FormatService formatService;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    @CommandLine.Parameters(index = "0", description = "(get-contexts | current-context | use-context)", arity = "1")
    public ConfigAction action;

    @CommandLine.Parameters(index="1", defaultValue = "", description = "Context", arity = "1")
    public String contextNameToSet;

    @Override
    public Integer call() throws Exception {
        if (action.equals(ConfigAction.CURRENT_CONTEXT)) {
            Map<String,Object> specs = new HashMap<>();
            specs.put("namespace", kafkactlConfig.getCurrentNamespace());
            specs.put("api", kafkactlConfig.getApi());
            specs.put("token", kafkactlConfig.getUserToken());

            Resource currentContextAsResource = Resource.builder()
                    .metadata(ObjectMeta.builder()
                            .name(configService.getCurrentContextName())
                            .build())
                    .spec(specs)
                    .build();

            formatService.displayList("Context", List.of(currentContextAsResource), "table");
            return 0;
        }

        if (action.equals(ConfigAction.GET_CONTEXTS)) {
            List<Resource> allContextsAsResources = new ArrayList<>();
            kafkactlConfig.getContexts().forEach(context -> {
                Map<String,Object> specs = new HashMap<>();
                specs.put("namespace", context.getContext().getNamespace());
                specs.put("api", context.getContext().getApi());
                specs.put("token", context.getContext().getUserToken());

                Resource currentContextAsResource = Resource.builder()
                        .metadata(ObjectMeta.builder()
                                .name(context.getName())
                                .build())
                        .spec(specs)
                        .build();

                allContextsAsResources.add(currentContextAsResource);
            });

            formatService.displayList("Context", allContextsAsResources, "table");
            return 0;
        }

        Optional<KafkactlConfig.Context> optionalContextToSet = configService.getContextByName(contextNameToSet);
        if (optionalContextToSet.isEmpty()) {
            System.out.println("error: no context exists with the name: " + contextNameToSet);
            return 1;
        }

        KafkactlConfig.Context contextToSet = optionalContextToSet.get();
        if (action.equals(ConfigAction.USE_CONTEXT)) {
            configService.updateConfigurationContext(contextToSet);
            System.out.println("Switched to context \"" + contextNameToSet + "\".");
            return 0;
        }

        return 1;
    }
}

enum ConfigAction {
    GET_CONTEXTS("get-contexts"),
    CURRENT_CONTEXT("current-context"),
    USE_CONTEXT("use-context");

    @Getter
    private final String realName;

    ConfigAction(String realName) {
        this.realName = realName;
    }

    @Override
    public String toString() {
        return realName;
    }
}
