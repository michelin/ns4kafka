package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SchemaTest {
    @Test
    void testEquals() {
        Schema original = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema-one")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .compatibility(Schema.Compatibility.BACKWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        Schema same = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema-one")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .compatibility(Schema.Compatibility.BACKWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        Schema different = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema-one")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .compatibility(Schema.Compatibility.BACKWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"}]}")
                        .build())
                .build();

        Schema differentByCompat = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema-one")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .compatibility(Schema.Compatibility.FORWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        Schema differentByMetadata = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema-two")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .compatibility(Schema.Compatibility.FORWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        assertEquals(original,same);
        assertNotEquals(original, different);
        assertNotEquals(original, differentByCompat);
        assertNotEquals(original, differentByMetadata);
    }

    @Test
    void testSchemaListEquals() {
        SchemaList original = SchemaList.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema-one")
                        .build())
                .build();

        SchemaList same = SchemaList.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema-one")
                        .build())
                .build();

        SchemaList different = SchemaList.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema-two")
                        .build())
                .build();

        assertEquals(original,same);
        assertNotEquals(original, different);
    }
}
