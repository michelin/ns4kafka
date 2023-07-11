/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.michelin.ns4kafka.services.clients.connect.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

public record ConnectorStateInfo(String name, ConnectorState connector, List<TaskState> tasks, ConnectorType type) {

    @Getter
    public abstract static class AbstractState {
        private final String state;
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private final String trace;
        @JsonProperty("worker_id")
        private final String workerId;

        AbstractState(String state, String workerId, String trace) {
            this.state = state;
            this.workerId = workerId;
            this.trace = trace;
        }
    }

    public static class ConnectorState extends AbstractState {
        public ConnectorState(@JsonProperty("state") String state, @JsonProperty("worker_id") String worker,
                              @JsonProperty("msg") String msg) {
            super(state, worker, msg);
        }
    }

    @Getter
    public static class TaskState extends AbstractState implements Comparable<TaskState> {
        private final int id;

        public TaskState(@JsonProperty("id") int id, @JsonProperty("state") String state, @JsonProperty("worker_id") String worker,
                         @JsonProperty("msg") String msg) {
            super(state, worker, msg);
            this.id = id;
        }

        @Override
        public int compareTo(TaskState that) {
            return Integer.compare(this.id, that.id);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof TaskState other))
                return false;
            return compareTo(other) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
