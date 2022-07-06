package com.michelin.ns4kafka.cli.client.predicates;

import com.michelin.ns4kafka.cli.models.Status;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.retry.annotation.RetryPredicate;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Optional;

public class RetryTimeoutPredicate implements RetryPredicate {
    /**
     * Detect timeout exceptions from Ns4kafka API
     * @param throwable the input argument
     * @return true if a retry is necessary, false otherwise
     */
    @Override
    public boolean test(Throwable throwable) {
        if (throwable instanceof HttpClientResponseException) {
            Optional<Status> statusOptional = ((HttpClientResponseException) throwable).getResponse().getBody(Status.class);
            if (statusOptional.isPresent()) {
                Status status = statusOptional.get();

                if (status.getDetails() != null && !status.getDetails().getCauses().isEmpty()) {
                    System.out.println("Received timeout from Ns4Kafka... retrying in few seconds...");

                    return status.getCode() == HttpResponseStatus.INTERNAL_SERVER_ERROR.code()
                            && status.getDetails().getCauses().stream().anyMatch(cause -> cause.contains("ReadTimeoutException"));
                }
            }
        }
        return false;
    }
}
