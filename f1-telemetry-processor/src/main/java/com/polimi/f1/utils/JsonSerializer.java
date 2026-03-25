package com.polimi.f1.utils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

// generic jackson serializer for kafka sinks, mirrors JsonDeserializer.
// converts pojos to json format (one compact json object per line).
// ex: PitStopEvaluationAlert -> {"driver":"VER","pitLapNumber":15,"result":"SUCCESS_UNDERCUT",...}
public class JsonSerializer<T> extends RichMapFunction<T, String> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public String map(T value) throws Exception {
        return objectMapper.writeValueAsString(value);
    }
}
