package com.michelin.ns4kafka.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ConfigService;
import com.michelin.ns4kafka.cli.services.FormatService;
import lombok.Getter;
import picocli.CommandLine;

import javax.inject.Inject;
import java.io.File;
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
    public String contextToSet;

    @Override
    public Integer call() throws Exception {
        if (action.equals(ConfigAction.CURRENT_CONTEXT)) {
            System.out.println(kafkactlConfig.getCurrentContext());
            return 0;
        }

        if (action.equals(ConfigAction.GET_CONTEXTS)) {
            List<Resource> allContextsAsResources = new ArrayList<>();
            kafkactlConfig.getContexts().forEach(context -> {
                Map<String,Object> specs = new HashMap<>();
                specs.put("api", context.getContext().getApi());
                specs.put("namespace", context.getContext().getCurrentNamespace());

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

        if (!configService.contextExists(contextToSet)) {
            System.out.println("error: no context exists with the name: " + contextToSet);
            return 1;
        }

        if (action.equals(ConfigAction.USE_CONTEXT)) {
            kafkactlConfig.setCurrentContext(contextToSet);
            ObjectMapper mapper = new YAMLMapper();
            mapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
            mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, true);
            mapper.writeValue(new File(kafkactlConfig.getConfigPath() + "/config.yml"), kafkactlConfig);
            System.out.println("Switched to context \"" + contextToSet + "\".");
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
