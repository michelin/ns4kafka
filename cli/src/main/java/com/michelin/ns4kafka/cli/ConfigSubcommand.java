package com.michelin.ns4kafka.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ConfigService;
import com.michelin.ns4kafka.cli.services.FormatService;
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

    @CommandLine.Parameters(index = "0", description = "(getcontexts | currentcontext | usecontext)", arity = "1")
    public ConfigAction action;

    @CommandLine.Parameters(index="1", defaultValue = "", description = "Context", arity = "1")
    public String contextToSet;

    @Override
    public Integer call() throws Exception {
        if (action.equals(ConfigAction.currentcontext)) {
            System.out.println(kafkactlConfig.getCurrentContext());
            return 0;
        }

        if (action.equals(ConfigAction.getcontexts)) {
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

        if (action.equals(ConfigAction.usecontext)) {
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
    getcontexts,
    currentcontext,
    usecontext
}
