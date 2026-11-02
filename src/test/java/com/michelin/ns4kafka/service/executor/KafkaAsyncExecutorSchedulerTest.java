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
package com.michelin.ns4kafka.service.executor;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

class KafkaAsyncExecutorSchedulerTest {
    @Test
    void shouldStartAndDisposeTableflowSynchronizationWithTheOtherExecutors() {
        var scheduler = spy(new KafkaAsyncExecutorScheduler(
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                new Ns4KafkaProperties.SchedulerProperties()));
        Disposable connector = mock(Disposable.class);
        Disposable roleBinding = mock(Disposable.class);
        Disposable tableflow = mock(Disposable.class);
        doReturn(connector).when(scheduler).scheduleConnectorSynchronization();
        doReturn(roleBinding).when(scheduler).scheduleRoleBindingSynchronization();
        doReturn(tableflow).when(scheduler).scheduleTableflowSynchronization();

        scheduler.onStartupEvent(null);
        scheduler.onDestroy();

        verify(scheduler).scheduleTableflowSynchronization();
        verify(connector).dispose();
        verify(roleBinding).dispose();
        verify(tableflow).dispose();
    }
}
