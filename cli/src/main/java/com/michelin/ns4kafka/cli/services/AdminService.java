package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.FormatUtils;
import com.michelin.ns4kafka.cli.LogSubcommand;
import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;

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
        try {
            return resourceClient.defaultRetrievesLogs(loginService.getAuthorization());
        } catch ( HttpClientResponseException e) {
            FormatUtils.displayError(e, "Logs", null);
        }
        return List.of();
    }

    public List<LogSubcommand.AuditLog> retrievesLogsFromDuration(Duration duration) {
        try {
            return resourceClient.retrievesLogsFromDuration(loginService.getAuthorization(), duration);
        } catch (HttpClientResponseException e) {
            FormatUtils.displayError(e, "Logs", null);
        }
        return List.of();
    }
}

