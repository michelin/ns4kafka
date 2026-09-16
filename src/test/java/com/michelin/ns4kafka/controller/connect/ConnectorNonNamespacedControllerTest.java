/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.controller.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.connect.Connector;
import com.michelin.ns4kafka.service.ConnectorService;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConnectorNonNamespacedControllerTest {
    @Mock
    ConnectorService connectorService;

    @InjectMocks
    ConnectorNonNamespacedController connectorController;

    @Test
    void shouldFindAll() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connector1").build())
                .build();
        Connector connector2 = Connector.builder()
                .metadata(Resource.Metadata.builder().name("connector2").build())
                .build();

        when(connectorService.findAllByPhase(null)).thenReturn(List.of(connector, connector2));

        Collection<Connector> actual = connectorController.listAll(null);

        assertEquals(List.of(connector, connector2), actual);
    }

    @Test
    void shouldFindAllByPhase() {
        Connector connector = Connector.builder()
                .metadata(Resource.Metadata.builder()
                        .name("connector1")
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .build();

        when(connectorService.findAllByPhase(Resource.Metadata.Phase.PENDING)).thenReturn(List.of(connector));

        Collection<Connector> actual = connectorController.listAll(Resource.Metadata.Phase.PENDING);

        assertEquals(List.of(connector), actual);
    }
}
