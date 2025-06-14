package com.michelin.ns4kafka.validation;

import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SubjectNameStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SchemaSubjectNameValidatorTest {

    @Test
    void testValidateSubjectName_TopicNameStrategy_Valid() {
        String subject = "mytopic-key";
        String schemaContent = "{\"name\":\"User\"}";
        boolean result = SchemaSubjectNameValidator.validateSubjectName(
                subject,
                List.of(SubjectNameStrategy.TOPIC_NAME),
                schemaContent,
                Schema.SchemaType.AVRO
        );
        assertTrue(result);
    }

    @Test
    void testValidateSubjectName_TopicNameStrategy_Invalid() {
        String subject = "mytopic";
        String schemaContent = "{\"name\":\"User\"}";
        boolean result = SchemaSubjectNameValidator.validateSubjectName(
                subject,
                List.of(SubjectNameStrategy.TOPIC_NAME),
                schemaContent,
                Schema.SchemaType.AVRO
        );
        assertFalse(result);
    }

    @Test
    void testValidateSubjectName_TopicRecordNameStrategy_Valid() {
        String subject = "mytopic-User";
        String schemaContent = "{\"name\":\"User\"}";
        boolean result = SchemaSubjectNameValidator.validateSubjectName(
                subject,
                List.of(SubjectNameStrategy.TOPIC_RECORD_NAME),
                schemaContent,
                Schema.SchemaType.AVRO
        );
        assertTrue(result);
    }

    @Test
    void testValidateSubjectName_RecordNameStrategy_Valid() {
        String subject = "User";
        String schemaContent = "{\"name\":\"User\"}";
        boolean result = SchemaSubjectNameValidator.validateSubjectName(
                subject,
                List.of(SubjectNameStrategy.RECORD_NAME),
                schemaContent,
                Schema.SchemaType.AVRO
        );
        assertTrue(result);
    }

    @Test
    void testExtractRecordName_Avro() {
        String schemaContent = "{\"name\":\"com.example.User\"}";
        Optional<String> recordName = SchemaSubjectNameValidator.extractRecordName(schemaContent, Schema.SchemaType.AVRO);
        assertTrue(recordName.isPresent());
        assertEquals("User", recordName.get());
    }

    @Test
    void testExtractTopicName_TopicNameStrategy() {
        Optional<String> topic = SchemaSubjectNameValidator.extractTopicName("mytopic-key", SubjectNameStrategy.TOPIC_NAME);
        assertTrue(topic.isPresent());
        assertEquals("mytopic", topic.get());
    }

    @Test
    void testExtractTopicName_TopicRecordNameStrategy() {
        Optional<String> topic = SchemaSubjectNameValidator.extractTopicName("mytopic-User", SubjectNameStrategy.TOPIC_RECORD_NAME);
        assertTrue(topic.isPresent());
        assertEquals("mytopic", topic.get());
    }
}