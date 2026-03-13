package com.polimi.f1.utils;

import org.apache.flink.api.common.functions.MapFunction;

// emits a csv header as the first row, then delegates to toCsvRow() for data rows.
// uses a boolean flag to emit the header exactly once per parallel instance.
// ex output: "driver,pitLapNumber,...\nVER,15,..."
public class CsvHeaderMapper<T> implements MapFunction<T, String> {

    private final String header;
    private final SerializableToCsvRow<T> toCsvRow;
    private boolean headerEmitted = false;

    public CsvHeaderMapper(String header, SerializableToCsvRow<T> toCsvRow) {
        this.header = header;
        this.toCsvRow = toCsvRow;
    }

    @Override
    public String map(T value) throws Exception {
        if (!headerEmitted) {
            headerEmitted = true;
            return header + "\n" + toCsvRow.apply(value);
        }
        return toCsvRow.apply(value);
    }
}
