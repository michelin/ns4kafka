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
package com.michelin.ns4kafka.validation;

import com.michelin.ns4kafka.model.schema.SubjectNameStrategy;
import java.util.List;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Getter;

/**
 * Valid subject name strategies for value and key. This class is used to validate the subject name strategies for Kafka
 * topics.
 */
@Getter
@Builder
public class AuthorizedSubjectNameStrategy {
    private List<SubjectNameStrategy> keyStrategies;
    private List<SubjectNameStrategy> valueStrategies;

    /**
     * Build default authorized subject name strategies.
     *
     * @return The default authorized subject name strategies
     */
    public static AuthorizedSubjectNameStrategy defaultStrategies() {
        return AuthorizedSubjectNameStrategy.builder()
                .keyStrategies(List.of(SubjectNameStrategy.defaultStrategy()))
                .valueStrategies(List.of(SubjectNameStrategy.defaultStrategy()))
                .build();
    }

    /**
     * Get all valid subject name strategies (key and value) without duplicates.
     *
     * @return List of all valid subject name strategies
     */
    public List<SubjectNameStrategy> all() {
        return Stream.of(valueStrategies, keyStrategies)
                .flatMap(List::stream)
                .distinct()
                .toList();
    }
}
