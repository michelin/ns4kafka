package com.michelin.ns4kafka.config;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.type.Argument;
import io.micronaut.serde.Decoder;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.exceptions.SerdeException;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;

@Factory
public class BeanFactoryConfig {
    @Bean
    public Deserializer<Number> serializerNumber() {
        return new Deserializer<>() {
            @Override
            public @Nullable Number deserialize(@NonNull Decoder decoder,
                                                @NonNull DecoderContext context,
                                                @NonNull Argument<? super Number> type) throws
                    IOException {
                try {
                    return NumberFormat.getInstance().parse(decoder.decodeString());
                } catch (ParseException e) {
                    throw new SerdeException("Error decoding number of type " + type + ":" + e.getMessage(), e);
                }
            }
        };
    }

    @Bean
    public Serializer<Number> serializer() {
        return (encoder, context, type, value) -> encoder.encodeString(NumberFormat.getInstance().format(value));
    }
}
