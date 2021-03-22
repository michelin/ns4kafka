package com.michelin.ns4kafka.services.connect.client.entities;

import lombok.Data;

@Data
public class ConnectorStatus {
    private ConnectorInfo info;
    private ConnectorStateInfo status;
}
