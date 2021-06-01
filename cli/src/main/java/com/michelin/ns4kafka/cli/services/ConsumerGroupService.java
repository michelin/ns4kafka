
package com.michelin.ns4kafka.cli.services;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.cli.client.ConsumerGroupClient;
import com.michelin.ns4kafka.cli.models.Resource;

@Singleton
public class ConsumerGroupService {

    @Inject
    public ConsumerGroupClient consumerGroupClient;
    @Inject
    public LoginService loginService;


    public Resource reset(String namespace, String group, Resource resource, boolean dryRun) {
        return consumerGroupClient.reset(loginService.getAuthorization(), namespace, group, resource, dryRun);
    }
}
