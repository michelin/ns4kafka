package com.michelin.ns4kafka.services.clients.connect.entities;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record ConnectorStatus(ConnectorInfo info, ConnectorStateInfo status) {
}
