package com.michelin.ns4kafka.services.clients.connect.entities;

/**
 * Connector status.
 *
 * @param info   Connector info
 * @param status Connector status info
 */
public record ConnectorStatus(ConnectorInfo info, ConnectorStateInfo status) {
}
