package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.models.Resource;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
     * From a given file path in a resource, load the content of the matching file into a new map attribute
     *
     * @param resource The resource
     * @param filePathAttributeName The name of the attribute containing the file path in the resource spec
     * @param mapAttributeName The name of the map attribute, containing the file content
     * @param fileContentAttributeName The key matching the file content
     */
    public void loadFileContent(Resource resource, String filePathAttributeName, String mapAttributeName, String fileContentAttributeName) {
        Map<String, String> mapFileContent = new LinkedHashMap<>();

        try {
            mapFileContent.put(fileContentAttributeName, Files.readString(new File((String) resource.getSpec().get(filePathAttributeName)).toPath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        resource.getSpec().put(mapAttributeName, mapFileContent);
    }
}
