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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AccessControlEntryAsyncExecutorTest {

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @InjectMocks
    AccessControlEntryAsyncExecutor aclAsyncExecutor;

    @Test
    void shouldConvertPublicAcl() {
        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-owner")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("*")
                        .build())
                .build();

        AclBinding aclBinding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "ns1-", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        "User:*",
                        "*",
                        AclOperation.fromString(acl.getSpec().getPermission().toString()),
                        AclPermissionType.ALLOW));

        assertEquals(aclBinding, aclAsyncExecutor.convertPublicAcl(acl));
    }
}
