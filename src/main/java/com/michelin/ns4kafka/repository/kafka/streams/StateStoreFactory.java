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
package com.michelin.ns4kafka.repository.kafka.streams;

import com.michelin.ns4kafka.model.*;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.repository.kafka.InternalTopic;
import io.micronaut.configuration.kafka.serde.JsonObjectSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.json.JsonObjectSerializer;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

@Factory
public class StateStoreFactory {
    private final InternalTopic internalTopic;
    private final JsonObjectSerializer jsonObjectSerializer;

    public StateStoreFactory(InternalTopic internalTopic, JsonObjectSerializer jsonObjectSerializer) {
        this.jsonObjectSerializer = jsonObjectSerializer;
        this.internalTopic = internalTopic;
    }

    @Singleton
    public KStream<String, Namespace> stateStoreFactory(ConfiguredStreamBuilder builder) {
        JsonObjectSerde<RoleBinding> roleBindingSerde = new JsonObjectSerde<>(jsonObjectSerializer, RoleBinding.class);
        builder.stream(internalTopic.roleBindingTopic(), Consumed.with(Serdes.String(), roleBindingSerde))
                .filterNot((key, _) -> key.equals("NOOP"))
                .toTable(Materialized.<String, RoleBinding, KeyValueStore<Bytes, byte[]>>as(
                                internalTopic.roleBindingTopic())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(roleBindingSerde));

        JsonObjectSerde<ResourceQuota> resourceQuotaSerde =
                new JsonObjectSerde<>(jsonObjectSerializer, ResourceQuota.class);
        builder.stream(internalTopic.resourceQuotaTopic(), Consumed.with(Serdes.String(), resourceQuotaSerde))
                .filterNot((key, _) -> key.equals("NOOP"))
                .toTable(Materialized.<String, ResourceQuota, KeyValueStore<Bytes, byte[]>>as(
                                internalTopic.resourceQuotaTopic())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(resourceQuotaSerde));

        JsonObjectSerde<AccessControlEntry> aclSerde =
                new JsonObjectSerde<>(jsonObjectSerializer, AccessControlEntry.class);
        builder.stream(internalTopic.aclTopic(), Consumed.with(Serdes.String(), aclSerde))
                .filterNot((key, _) -> key.equals("NOOP"))
                .toTable(Materialized.<String, AccessControlEntry, KeyValueStore<Bytes, byte[]>>as(
                                internalTopic.aclTopic())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(aclSerde));

        JsonObjectSerde<ConnectCluster> connectClusterSerde =
                new JsonObjectSerde<>(jsonObjectSerializer, ConnectCluster.class);
        builder.stream(internalTopic.connectClusterTopic(), Consumed.with(Serdes.String(), connectClusterSerde))
                .filterNot((key, _) -> key.equals("NOOP"))
                .toTable(Materialized.<String, ConnectCluster, KeyValueStore<Bytes, byte[]>>as(
                                internalTopic.connectClusterTopic())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(connectClusterSerde));

        JsonObjectSerde<Connector> connectorSerde = new JsonObjectSerde<>(jsonObjectSerializer, Connector.class);
        builder.stream(internalTopic.connectorTopic(), Consumed.with(Serdes.String(), connectorSerde))
                .filterNot((key, _) -> key.equals("NOOP"))
                .toTable(
                        Materialized.<String, Connector, KeyValueStore<Bytes, byte[]>>as(internalTopic.connectorTopic())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(connectorSerde));

        JsonObjectSerde<KafkaStream> kafkaStreamSerde = new JsonObjectSerde<>(jsonObjectSerializer, KafkaStream.class);
        builder.stream(internalTopic.streamTopic(), Consumed.with(Serdes.String(), kafkaStreamSerde))
                .filterNot((key, _) -> key.equals("NOOP"))
                .toTable(Materialized.<String, KafkaStream, KeyValueStore<Bytes, byte[]>>as(internalTopic.streamTopic())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(kafkaStreamSerde));

        JsonObjectSerde<Topic> topicSerde = new JsonObjectSerde<>(jsonObjectSerializer, Topic.class);
        builder.stream(internalTopic.topicTopic(), Consumed.with(Serdes.String(), topicSerde))
                .filterNot((key, _) -> key.equals("NOOP"))
                .toTable(Materialized.<String, Topic, KeyValueStore<Bytes, byte[]>>as(internalTopic.topicTopic())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(topicSerde));

        JsonObjectSerde<Namespace> namespaceSerde = new JsonObjectSerde<>(jsonObjectSerializer, Namespace.class);
        return builder.stream(internalTopic.namespaceTopic(), Consumed.with(Serdes.String(), namespaceSerde))
                .filterNot((key, _) -> key.equals("NOOP"))
                .toTable(
                        Materialized.<String, Namespace, KeyValueStore<Bytes, byte[]>>as(internalTopic.namespaceTopic())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(namespaceSerde))
                .toStream();
    }
}
