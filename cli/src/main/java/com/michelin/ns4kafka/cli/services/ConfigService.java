package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.KafkactlConfig;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import picocli.CommandLine;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Singleton
public class ConfigService {
    /**
     * The current Kafkactl configuration
     */
    @Inject
    public KafkactlConfig kafkactlConfig;

    /**
     * Login service
     */
    @Inject
    public LoginService loginService;

    /**
     * Return the name of the current context according to the current api, namespace
     * and token properties
     *
     * @return The current context name
     */
    public String getCurrentContextName() {
        return kafkactlConfig.getContexts()
                .stream()
                .filter(context -> context.getContext().getApi().equals(kafkactlConfig.getApi()) &&
                        context.getContext().getNamespace().equals(kafkactlConfig.getCurrentNamespace()) &&
                        context.getContext().getUserToken().equals(kafkactlConfig.getUserToken()))
                .findFirst()
                .map(KafkactlConfig.Context::getName)
                .orElse(null);
    }

    /**
     * Get the current context infos if it exists
     *
     * @return The current context
     */
    public Optional<KafkactlConfig.Context> getContextByName(String name) {
        return kafkactlConfig.getContexts()
                .stream()
                .filter(context -> context.getName().equals(name))
                .findFirst();
    }

    /**
     * Update the current configuration context with the given new context
     *
     * @param contextToSet The context to set
     * @throws IOException Any exception during file writing
     */
    public void updateConfigurationContext(KafkactlConfig.Context contextToSet) throws IOException {
        Map<String, Object> currentContext = new LinkedHashMap<>();
        currentContext.put("currentNamespace", contextToSet.getContext().getNamespace());
        currentContext.put("api", contextToSet.getContext().getApi());
        currentContext.put("userToken", contextToSet.getContext().getUserToken());
        currentContext.put("contexts", kafkactlConfig.getContexts());

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("kafkactl", currentContext);

        Representer representer = new Representer();
        representer.addClassTag(KafkactlConfig.Context.class, Tag.MAP);

        DumperOptions options = new DumperOptions();
        options.setIndent(2);
        options.setPrettyFlow(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        Yaml yamlMapper = new Yaml(representer, options);
        FileWriter writer = new FileWriter(kafkactlConfig.getConfigPath() + "/config.yml");
        yamlMapper.dump(config, writer);

        loginService.deleteJWTfile();
    }
}
