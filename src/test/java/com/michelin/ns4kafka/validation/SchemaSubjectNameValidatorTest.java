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

import static org.junit.jupiter.api.Assertions.*;

import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SubjectNameStrategy;
import com.michelin.ns4kafka.service.SchemaService;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SchemaSubjectNameValidatorTest {

    @Test
    void testValidateSubjectName_TopicNameStrategy_Valid() {
        String subject = "mytopic-key";
        String schemaContent = "{\"name\":\"User\"}";
        boolean result = SchemaService.validateSubjectName(
                subject, List.of(SubjectNameStrategy.TOPIC_NAME), schemaContent, Schema.SchemaType.AVRO);
        assertTrue(result);
    }

    @Test
    void testValidateSubjectName_TopicNameStrategy_Invalid() {
        String subject = "mytopic";
        String schemaContent = "{\"name\":\"User\", \"namespace\":\"namespace\"}";
        boolean result = SchemaService.validateSubjectName(
                subject, List.of(SubjectNameStrategy.TOPIC_NAME), schemaContent, Schema.SchemaType.AVRO);
        assertFalse(result);
    }

    @Test
    void testValidateSubjectName_TopicRecordNameStrategy_Valid() {
        String subject = "mytopic-namespace.User";
        String schemaContent = "{\"name\":\"User\", \"namespace\":\"namespace\"}";
        boolean result = SchemaService.validateSubjectName(
                subject, List.of(SubjectNameStrategy.TOPIC_RECORD_NAME), schemaContent, Schema.SchemaType.AVRO);
        assertTrue(result);
    }

    @Test
    void testValidateSubjectName_RecordNameStrategy_Valid() {
        String subject = "User";
        String schemaContent = "{\"name\":\"User\"}";
        boolean result = SchemaService.validateSubjectName(
                subject, List.of(SubjectNameStrategy.RECORD_NAME), schemaContent, Schema.SchemaType.AVRO);
        assertTrue(result);
    }

    @Test
    void testExtractRecordName_Avro() {
        String schemaContent = "{\"name\":\"com.example.User\"}";
        Optional<String> recordName = SchemaService.extractRecordName(schemaContent, Schema.SchemaType.AVRO);
        assertTrue(recordName.isPresent());
        assertEquals("com.example.User", recordName.get());
    }

    @Test
    void testExtractTopicName_TopicNameStrategy() {
        Optional<String> topic =
                SchemaService.extractTopicName("mytopic-with-dashes-key", SubjectNameStrategy.TOPIC_NAME);
        assertTrue(topic.isPresent());
        assertEquals("mytopic-with-dashes", topic.get());
    }

    @Test
    void testExtractTopicName_TopicRecordNameStrategy() {
        Optional<String> topic =
                SchemaService.extractTopicName("mytopic-with-dashes-User", SubjectNameStrategy.TOPIC_RECORD_NAME);
        assertTrue(topic.isPresent());
        assertEquals("mytopic-with-dashes", topic.get());
    }
}
