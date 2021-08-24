package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.LogSubcommand;
import com.michelin.ns4kafka.cli.client.ClusterResourceClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.List;

@Singleton
public class AdminService {

    @Inject
    public ClusterResourceClient resourceClient;

    @Inject
    LoginService loginService;

    public List<LogSubcommand.AuditLog> defaultRetrievesLogs() {
        return resourceClient.defaultRetrievesLogs(loginService.getAuthorization());
    }

    public List<LogSubcommand.AuditLog> retrievesLogsFromDuration(Duration duration) {
        return resourceClient.retrievesLogsFromDuration(loginService.getAuthorization(), duration);
    }

}
