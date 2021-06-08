package com.example.sbremote.batch;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class CustomJsonDeserializer<T> implements Deserializer<T> {
    ObjectMapper objectMapper;

    public CustomJsonDeserializer(){
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, false);
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            T o = this.objectMapper.readValue(bytes, new TypeReference<T>() {
            });
            log.info(o.toString());
            return o;
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {

    }
}