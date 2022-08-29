package com.michelin.ns4kafka.cli.client.predicates;

import com.michelin.ns4kafka.cli.models.Status;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.retry.annotation.RetryPredicate;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Optional;

public class RetryTimeoutPredicate implements RetryPredicate {
    /**
     * Detect when Ns4Kafka return a timeout exception (e.g. when deploying schemas, connectors)
     * @param throwable the input argument
     * @return true if a retry is necessary, false otherwise
     */
    @Override
    public boolean test(Throwable throwable) {
        if (throwable instanceof HttpClientResponseException) {
            Optional<Status> statusOptional = ((HttpClientResponseException) throwable).getResponse().getBody(Status.class);
            if (statusOptional.isPresent() && statusOptional.get().getDetails() != null
                    && !statusOptional.get().getDetails().getCauses().isEmpty()
                    && HttpResponseStatus.INTERNAL_SERVER_ERROR.code() == statusOptional.get().getCode()
                    && statusOptional.get().getDetails().getCauses().stream().anyMatch(cause -> cause.contains("Read Timeout"))) {
                System.out.println("Read timeout... retrying...");
                return true;
            }
        }
        return false;
    }
}
