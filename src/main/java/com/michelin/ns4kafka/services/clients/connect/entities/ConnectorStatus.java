package com.michelin.ns4kafka.services.clients.connect.entities;

public record ConnectorStatus(ConnectorInfo info, ConnectorStateInfo status) {
}
