package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.models.Resource;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Singleton
public class FileService {

    public List<File> computeYamlFileList(File fileOrDirectory, boolean recursive) {
        return listAllFiles(new File[]{fileOrDirectory}, recursive)
                .collect(Collectors.toList());
    }

    public List<Resource> parseResourceListFromFiles(List<File> files) {
        return files.stream()
                .map(File::toPath)
                .map(path -> {
                    try{
                        return Files.readString(path);
                    } catch (IOException e) {
                        // checked to unchecked
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(this::parseResourceStreamFromString)
                .collect(Collectors.toList());
    }

    public List<Resource> parseResourceListFromString(String content){
        return parseResourceStreamFromString(content)
                .collect(Collectors.toList());
    }
    private Stream<Resource> parseResourceStreamFromString(String content){
        Yaml yaml = new Yaml(new Constructor(Resource.class));
        return StreamSupport.stream(yaml.loadAll(content).spliterator(), false)
                .map(o -> (Resource) o);
    }

    private Stream<File> listAllFiles(File[] rootDir, boolean recursive) {
        return Arrays.stream(rootDir)
                .flatMap(currentElement -> {
                    if (currentElement.isDirectory()) {
                        File[] files = currentElement.listFiles(file -> file.isFile() && (file.getName().endsWith(".yaml") || file.getName().endsWith(".yml")));
                        Stream<File> directories = recursive ? listAllFiles(currentElement.listFiles(File::isDirectory), true) : Stream.empty();
                        return Stream.concat(Stream.of(files), directories);
                    } else {
                        return Stream.of(currentElement);
                    }
                });
    }

    /**
     * Load the schema file content
     *
     * @param resource The resource
     */
    public void loadSchemaFileContent(Resource resource) throws IOException {
        resource.getSpec().put("schema",
                Files.readString(new File((String) resource.getSpec().get("schema")).toPath()));
    }
}
