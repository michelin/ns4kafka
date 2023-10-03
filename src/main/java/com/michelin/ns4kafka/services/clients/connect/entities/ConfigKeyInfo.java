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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Connector configuration information.
 *
 * @param name          Name
 * @param type          Type
 * @param required      Required
 * @param defaultValue  Default value
 * @param importance    Importance
 * @param documentation Documentation
 * @param group         Group
 * @param orderInGroup  Order in group
 * @param width         Width
 * @param displayName   Display name
 * @param dependents    Dependents
 */
public record ConfigKeyInfo(String name, String type, @JsonProperty("required") boolean required,
                            @JsonProperty("default_value") String defaultValue, String importance,
                            String documentation, String group, @JsonProperty("order_in_group") int orderInGroup,
                            String width, @JsonProperty("display_name") String displayName, List<String> dependents) {
}
