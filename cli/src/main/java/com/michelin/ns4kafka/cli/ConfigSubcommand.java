package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ConfigService;
import com.michelin.ns4kafka.cli.services.FormatService;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

    @CommandLine.Parameters(index = "0", description = "(pause | resume | restart)", arity = "1")
    public ConfigAction action;

    @Override
    public Integer call() throws Exception {
        KafkactlConfig.Context currentContext = configService.getCurrentContext();
        if (currentContext == null) {
            System.out.println("Context " + kafkactlConfig.getCurrentContext() + " does not exist");
            return 1;
        }

        if (action.equals(ConfigAction.currentcontext)) {
            Map<String,Object> specs = new HashMap<>();
            specs.put("api", currentContext.getContext().getApi());
            specs.put("namespace", currentContext.getContext().getCurrentNamespace());

            Resource currentContextAsResource = Resource.builder()
                    .metadata(ObjectMeta.builder()
                            .name(currentContext.getName())
                            .build())
                    .spec(specs)
                    .build();

            formatService.displayList("Context", Collections.singletonList(currentContextAsResource), "table");
            return 0;
        }

        return 1;
    }
}

enum ConfigAction {
    getcontexts,
    currentcontext,
    setcontext
}
