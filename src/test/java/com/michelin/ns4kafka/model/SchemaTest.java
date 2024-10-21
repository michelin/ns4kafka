package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.michelin.ns4kafka.model.schema.Schema;
import org.junit.jupiter.api.Test;

class SchemaTest {
    @Test
    void shouldBeEqual() {
        Schema original = Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.schema-one")
                .build())
            .spec(Schema.SchemaSpec.builder()
                .compatibility(Schema.Compatibility.BACKWARD)
                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\","
                    + "\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":"
                    + "[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                    + "\"default\":null,\"doc\":\"First name of the person\"},"
                    + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                    + "\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":"
                    + "[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],"
                    + "\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                .build())
            .build();

        Schema same = Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.schema-one")
                .build())
            .spec(Schema.SchemaSpec.builder()
                .compatibility(Schema.Compatibility.BACKWARD)
                .schema(
                    "{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\","
                        + "\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":"
                        + "[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                        + "\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":"
                        + "[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},"
                        + "{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\","
                        + "\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                        + "\"doc\":\"Date of birth of the person\"}]}")
                .build())
            .build();

        assertEquals(original, same);

        Schema different = Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.schema-one")
                .build())
            .spec(Schema.SchemaSpec.builder()
                .compatibility(Schema.Compatibility.BACKWARD)
                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\""
                    + ",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":"
                    + "[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                    + "\"default\":null,\"doc\":\"First name of the person\"},"
                    + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],"
                    + "\"default\":null,\"doc\":\"Last name of the person\"}]}")
                .build())
            .build();

        assertNotEquals(original, different);

        Schema differentByCompat = Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.schema-one")
                .build())
            .spec(Schema.SchemaSpec.builder()
                .compatibility(Schema.Compatibility.FORWARD)
                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\","
                    + "\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":"
                    + "[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                    + "\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":"
                    + "[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},"
                    + "{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\","
                    + "\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                    + "\"doc\":\"Date of birth of the person\"}]}")
                .build())
            .build();

        assertNotEquals(original, differentByCompat);

        Schema differentByMetadata = Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.schema-two")
                .build())
            .spec(Schema.SchemaSpec.builder()
                .compatibility(Schema.Compatibility.FORWARD)
                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\","
                    + "\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":"
                    + "[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                    + "\"default\":null,\"doc\":\"First name of the person\"},"
                    + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                    + "\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\","
                    + "\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],"
                    + "\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                .build())
            .build();

        assertNotEquals(original, differentByMetadata);
    }

    @Test
    void shouldSchemaBeEqual() {
        Schema original = Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.schema-one")
                .build())
            .build();

        Schema same = Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.schema-one")
                .build())
            .build();

        Schema different = Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.schema-two")
                .build())
            .build();

        assertEquals(original, same);
        assertNotEquals(original, different);
    }
}
