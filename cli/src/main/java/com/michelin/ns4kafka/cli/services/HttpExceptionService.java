package com.michelin.ns4kafka.cli.services;

import java.util.List;
import java.util.Optional;

import javax.inject.Singleton;

import com.michelin.ns4kafka.cli.models.Status;
import com.michelin.ns4kafka.cli.models.Status.StatusCause;

import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;

@Singleton
public class HttpExceptionService {

    public void printError(HttpClientResponseException e) {

        Optional<Status> statusOptional = e.getResponse().getBody(Status.class);
        if (statusOptional.isPresent()) {
            var status = statusOptional.get();
            var details = status.getDetails();
            System.out.println(status.getMessage());

            if (details != null) {

                List<StatusCause> causes = details.getCauses();
                if (causes != null && !causes.isEmpty()) {
                    displayAsTable(causes);
                }
            }

        } else {
            System.out.println("Error");

        }
    }

    private void displayAsTable(List<StatusCause> causes) {
        CommandLine.Help.TextTable tt = CommandLine.Help.TextTable.forColumns(
                CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO),
                new CommandLine.Help.Column[]
                        {
                                new CommandLine.Help.Column(50, 2, CommandLine.Help.Column.Overflow.SPAN),
                        });
        tt.addRowValues("MESSAGES");
        causes.forEach(cause -> tt.addRowValues(cause.getMessage()));
        System.out.println(tt);
    }
}
