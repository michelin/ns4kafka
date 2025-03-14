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
package com.michelin.ns4kafka.service.client.schema.entities;

import com.michelin.ns4kafka.model.schema.Schema;
import java.util.List;
import lombok.Builder;

/**
 * Schema response.
 *
 * @param id The id
 * @param version The version
 * @param subject The subject
 * @param schema The schema
 * @param schemaType The schema type
 * @param references The schema references
 */
@Builder
public record SchemaResponse(
        Integer id,
        Integer version,
        String subject,
        String schema,
        String schemaType,
        List<Schema.SchemaSpec.Reference> references) {}
