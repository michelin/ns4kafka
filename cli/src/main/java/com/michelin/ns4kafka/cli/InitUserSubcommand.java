package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.YmlWriterService;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

@Command(name = "init", description = "Init a namespace from an existing user")
public class InitUserSubcommand implements Callable<Integer> {

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    LoginService loginService;
    
    @Inject
    YmlWriterService ymlWriterService;

    @CommandLine.ParentCommand
    KafkactlCommand kafkactlCommand;

    @Option(names = {"-u", "--user"}, required = true, description = "The Kafka user to init")
    String user;
    @Option(names = {"-c", "--cluster"}, required = true, description = "The Kafka cluster belonging to the user to init")
    String cluster;
     
    
    public Integer call() throws IOException {

        // 1. Authent
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            return 1;
        }

      
        try {
            String defaultNamespace = kafkactlCommand.optionalNamespace.get();
            List<Resource> list = namespacedClient.list(defaultNamespace, cluster, user, loginService.getAuthorization());
            
            ymlWriterService.writeYaml(list);

        } catch (HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch (status) {
                case BAD_REQUEST:
                    System.err.println(e.getMessage());
                    return 2;
                default:
                    System.err.println("Resource failed with message : " + e.getMessage());
            }
            return 1;
        }  catch (NoSuchElementException e){
            System.err.println(Ansi.AUTO.string("@|bold,red Missing required option: '--namespace=<namespace>' |@"));
            return 1;
        }
        return 0;
    }
}
