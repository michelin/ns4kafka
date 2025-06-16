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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SubjectNameStrategy;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** Validator for schema subject names based on different naming strategies. */
@Slf4j
public final class SchemaSubjectNameValidator {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private SchemaSubjectNameValidator() {}

    /**
     * Validates that a schema subject name follows the specified naming strategy.
     *
     * @param subjectName The schema subject name to validate
     * @param schemaContent The schema content (for extracting record names)
     * @param schemaType The schema type (AVRO, JSON, PROTOBUF)
     * @return true if the subject name is valid for any of the strategies, false otherwise
     */
    public static boolean validateSubjectName(
            String subjectName,
            List<SubjectNameStrategy> validStrategies,
            String schemaContent,
            Schema.SchemaType schemaType) {
        if (subjectName == null || subjectName.trim().isEmpty()) {
            return false;
        }
        for (SubjectNameStrategy strategy : validStrategies) {
            if (validateSubjectNameWithStrategy(subjectName, strategy, schemaContent, schemaType)) {
                return true;
            }
        }
        return false;
    }

    public static boolean validateSubjectNameWithStrategy(
            String subjectName, SubjectNameStrategy strategy, String schemaContent, Schema.SchemaType schemaType) {
        switch (strategy) {
            case TOPIC_NAME:
                String topicName = extractTopicName(subjectName, strategy).orElse("");
                return subjectName.equals(topicName + "-key") || subjectName.equals(topicName + "-value");
            case TOPIC_RECORD_NAME:
                String topicName2 = extractTopicName(subjectName, strategy).orElse("");
                Optional<String> recordName = extractRecordName(schemaContent, schemaType);
                return recordName.isPresent() && subjectName.equals(topicName2 + "-" + recordName.get());
            case RECORD_NAME:
                Optional<String> recordNameOnly = extractRecordName(schemaContent, schemaType);
                return recordNameOnly.isPresent() && subjectName.equals(recordNameOnly.get());
            default:
                return false;
        }
    }

    /**
     * Extracts the record name from schema content based on schema type.
     *
     * @param schemaContent The schema content as string
     * @param schemaType The type of schema (AVRO, JSON, PROTOBUF)
     * @return Optional containing the record name if found
     */
    public static Optional<String> extractRecordName(String schemaContent, Schema.SchemaType schemaType) {
        if (schemaContent == null || schemaContent.trim().isEmpty()) {
            return Optional.empty();
        }

        try {
            switch (schemaType) {
                case AVRO:
                    return extractAvroRecordName(schemaContent);
                default:
                    log.warn("Unsupported schema type for record name extraction: {}", schemaType);
                    return Optional.empty();
            }
        } catch (Exception e) {
            log.error("Failed to extract record name from schema content", e);
            return Optional.empty();
        }
    }

    /** Extracts record name from AVRO schema. Looks for the "name" field in the root record type. */
    private static Optional<String> extractAvroRecordName(String schemaContent) {
        try {
            JsonNode schemaNode = OBJECT_MAPPER.readTree(schemaContent);
            if (schemaNode.has("name")) {
                String name = schemaNode.get("name").asText();
                return Optional.of(extractSimpleNameFromQualified(name));
            }
        } catch (Exception e) {
            log.debug("Failed to parse AVRO schema as JSON", e);
        }
        return Optional.empty();
    }

    /** Extracts the simple name from a potentially fully qualified name. For example: "com.example.User" -> "User" */
    private static String extractSimpleNameFromQualified(String qualifiedName) {
        if (qualifiedName == null || qualifiedName.trim().isEmpty()) {
            return qualifiedName;
        }

        int lastDotIndex = qualifiedName.lastIndexOf('.');
        if (lastDotIndex >= 0 && lastDotIndex < qualifiedName.length() - 1) {
            return qualifiedName.substring(lastDotIndex + 1);
        }

        return qualifiedName;
    }

    /**
     * Extracts the topic name from a subject name based on the naming strategy.
     *
     * @param subjectName The subject name (assumed to be not empty)
     * @param strategy The naming strategy
     * @return The topic name if it can be determined
     */
    public static Optional<String> extractTopicName(String subjectName, SubjectNameStrategy strategy) {
        switch (strategy) {
            case TOPIC_NAME:
                return Optional.of(subjectName.replaceAll("(-key|-value)$", ""));
            case TOPIC_RECORD_NAME:
                String[] parts = subjectName.split("-");
                return parts.length < 2 ? Optional.empty() : Optional.of(parts[0]);
            default:
                return Optional.empty();
        }
    }
}
