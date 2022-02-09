package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.KafkactlConfig;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import picocli.CommandLine;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

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
        Yaml yaml = new Yaml();
        File initialFile = new File(kafkactlConfig.getConfigPath() + "/config.yml");
        InputStream targetStream = new FileInputStream(initialFile);
        Map<String, LinkedHashMap<String, Object>> rootNodeConfig = yaml.load(targetStream);

        LinkedHashMap<String, Object> kafkactlNodeConfig = rootNodeConfig.get("kafkactl");
        kafkactlNodeConfig.put("current-namespace", contextToSet.getContext().getNamespace());
        kafkactlNodeConfig.put("api", contextToSet.getContext().getApi());
        kafkactlNodeConfig.put("user-token", contextToSet.getContext().getUserToken());

        DumperOptions options = new DumperOptions();
        options.setIndent(2);
        options.setPrettyFlow(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        Yaml yamlMapper = new Yaml(options);
        FileWriter writer = new FileWriter(kafkactlConfig.getConfigPath() + "/config.yml");
        yamlMapper.dump(rootNodeConfig, writer);

        loginService.deleteJWTfile();
    }
}
