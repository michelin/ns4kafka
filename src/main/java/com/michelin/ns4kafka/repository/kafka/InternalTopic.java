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
package com.michelin.ns4kafka.repository.kafka;

import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import jakarta.inject.Singleton;
import java.util.List;

@Singleton
public class InternalTopic {
    public static final String ACL = "access-control-entries";
    public static final String CONNECT_CLUSTER = "connect-workers";
    public static final String CONNECTOR = "connectors";
    public static final String NAMESPACE = "namespaces";
    public static final String RESOURCE_QUOTA = "resource-quotas";
    public static final String ROLE_BINDING = "role-bindings";
    public static final String STREAM = "streams";
    public static final String TOPIC = "topics";

    private final Ns4KafkaProperties ns4KafkaProperties;

    public InternalTopic(Ns4KafkaProperties ns4KafkaProperties) {
        this.ns4KafkaProperties = ns4KafkaProperties;
    }

    public String aclTopic() {
        return ns4KafkaProperties.getStore().getKafka().getTopics().getPrefix() + "." + ACL;
    }

    public String connectClusterTopic() {
        return ns4KafkaProperties.getStore().getKafka().getTopics().getPrefix() + "." + CONNECT_CLUSTER;
    }

    public String connectorTopic() {
        return ns4KafkaProperties.getStore().getKafka().getTopics().getPrefix() + "." + CONNECTOR;
    }

    public String namespaceTopic() {
        return ns4KafkaProperties.getStore().getKafka().getTopics().getPrefix() + "." + NAMESPACE;
    }

    public String resourceQuotaTopic() {
        return ns4KafkaProperties.getStore().getKafka().getTopics().getPrefix() + "." + RESOURCE_QUOTA;
    }

    public String roleBindingTopic() {
        return ns4KafkaProperties.getStore().getKafka().getTopics().getPrefix() + "." + ROLE_BINDING;
    }

    public String streamTopic() {
        return ns4KafkaProperties.getStore().getKafka().getTopics().getPrefix() + "." + STREAM;
    }

    public String topicTopic() {
        return ns4KafkaProperties.getStore().getKafka().getTopics().getPrefix() + "." + TOPIC;
    }

    public List<String> all() {
        return List.of(
                aclTopic(),
                connectClusterTopic(),
                connectorTopic(),
                namespaceTopic(),
                resourceQuotaTopic(),
                roleBindingTopic(),
                streamTopic(),
                topicTopic());
    }
}
