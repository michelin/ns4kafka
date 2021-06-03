package com.michelin.ns4kafka.cli.services;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.cli.client.TopicClient;

import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;


@Singleton
public class TopicService {

    @Inject
    public TopicClient topicClient;
    @Inject
    LoginService loginService;

    public boolean empty(String namespace, String topic, boolean dryrun) {
        try {
            topicClient.empty(loginService.getAuthorization(),namespace, topic, dryrun);
            return true;
        } catch (HttpClientResponseException e) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,red FAILED |@") + topic + CommandLine.Help.Ansi.AUTO.string("@|bold,red failed with message : |@") + e.getMessage());
        }
        return false;
    }

}
