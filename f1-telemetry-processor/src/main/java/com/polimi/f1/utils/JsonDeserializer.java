package com.polimi.f1.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

// generic jackson deserializer, reusable across all event types.
// creates one ObjectMapper per parallel instance (not serializable, so transient + open()).
public class JsonDeserializer<T> extends RichFlatMapFunction<String, T> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonDeserializer.class);

    private final Class<T> targetClass;
    private transient ObjectMapper mapper;

    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    private String recordType() {
        return targetClass.getSimpleName();
    }

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String value, Collector<T> out) {
        if (value == null || value.isBlank()) {
            LOG.warn("Skipping empty {} record", recordType());
            return;
        }

        try {
            out.collect(mapper.readValue(value, targetClass));
        } catch (JsonProcessingException e) {
            LOG.warn("Skipping malformed {} record: {}", recordType(), value, e);
        } catch (RuntimeException e) {
            LOG.warn("Skipping unreadable {} record: {}", recordType(), value, e);
        }
    }
}
