package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.models.Resource;
import org.apache.commons.lang3.RegExUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import javax.inject.Singleton;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

@Singleton
public class YmlWriterService {

    Yaml yaml;
    
    StringWriter writer;
    
    public YmlWriterService() {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        yaml = new Yaml(options);
        writer = new StringWriter();

    }
    
    public void writeYaml(Resource resource){
        yaml.dump(resource, writer);
        System.out.println(RegExUtils.replaceAll(writer.toString(),
                "!!com.michelin.ns4kafka.cli.models.Resource", "---"));
    }

    
    public void writeYaml(List<Resource> resources){
        yaml.dump(resources, writer);
        System.out.println(RegExUtils.replaceAll(writer.toString(),
                "- !!com.michelin.ns4kafka.cli.models.Resource", "---"));
    }
}
