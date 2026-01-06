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
package com.michelin.ns4kafka.property;

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

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.security.auth.local.LocalUser;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.convert.format.MapFormat;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/** Ns4Kafka properties. */
@Getter
@Setter
@ConfigurationProperties("ns4kafka")
public class Ns4KafkaProperties {
    private AkhqProperties akhq = new AkhqProperties();
    private ConfluentCloudProperties confluentCloud = new ConfluentCloudProperties();
    private SecurityProperties security = new SecurityProperties();
    private StoreProperties store = new StoreProperties();
    private String version;

    @Getter
    @Setter
    @ConfigurationProperties("akhq")
    public static class AkhqProperties {
        private String groupLabel;
        private String groupDelimiter = ",";
        private Map<AccessControlEntry.ResourceType, String> roles;
        private List<String> formerRoles;

        private String adminGroup;
        private Map<AccessControlEntry.ResourceType, String> adminRoles;
        private List<String> formerAdminRoles;
    }

    @Getter
    @Setter
    @ConfigurationProperties("confluent-cloud")
    public static class ConfluentCloudProperties {
        private StreamCatalogProperties streamCatalog = new StreamCatalogProperties();

        @Getter
        @Setter
        @ConfigurationProperties("stream-catalog")
        public static class StreamCatalogProperties {
            private int pageSize = 500;
            private boolean syncCatalog;
        }
    }

    @Getter
    @Setter
    @ConfigurationProperties("security")
    public static class SecurityProperties {
        private List<LocalUser> localUsers;
        private String adminGroup;
        private String aes256EncryptionKey;
    }

    @Getter
    @Setter
    @ConfigurationProperties("store")
    public static class StoreProperties {
        private KafkaProperties kafka = new KafkaProperties();

        @Getter
        @Setter
        @ConfigurationProperties("kafka")
        public static class KafkaProperties {
            private int initTimeout;
            private TopicsProperties topics = new TopicsProperties();

            @Getter
            @Setter
            @ConfigurationProperties("topics")
            public static class TopicsProperties {
                private String prefix;
                private int replicationFactor;

                @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
                private Map<String, String> props;
            }
        }
    }
}
