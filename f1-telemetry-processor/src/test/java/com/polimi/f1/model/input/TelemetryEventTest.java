package com.polimi.f1.model.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

// verifies parseEventTime handles both iso-8601 variants the python producer may emit:
//   1. with timezone suffix ("Z")         -> Instant.parse path
//   2. without timezone (bare datetime)   -> LocalDateTime.parse path, assumed UTC
//   3. invalid/empty/null strings          -> returns 0L (safe fallback)
class TelemetryEventTest {

    // "2023-09-03T13:05:12.003Z" -> 1693746312003 via Instant.parse
    @Test
    void parseEventTime_isoWithZ() {
        long result = TelemetryEvent.parseEventTime("2023-09-03T13:05:12.003Z");
        assertEquals(1693746312003L, result);
    }

    // "2023-09-03T13:05:12.003" -> same epoch millis via LocalDateTime fallback (UTC)
    @Test
    void parseEventTime_isoWithoutZ() {
        long result = TelemetryEvent.parseEventTime("2023-09-03T13:05:12.003");
        assertEquals(1693746312003L, result);
    }

    // whole-second timestamp without fractional part
    @Test
    void parseEventTime_wholeSeconds() {
        long result = TelemetryEvent.parseEventTime("2023-01-01T00:00:00Z");
        assertEquals(1672531200000L, result);
    }

    // null input should return 0 (safe default, flink watermarks treat 0 as "no event time")
    @Test
    void parseEventTime_null() {
        assertEquals(0L, TelemetryEvent.parseEventTime(null));
    }

    // empty string should return 0
    @Test
    void parseEventTime_empty() {
        assertEquals(0L, TelemetryEvent.parseEventTime(""));
    }

    // unparseable garbage should return 0
    @Test
    void parseEventTime_invalid() {
        assertEquals(0L, TelemetryEvent.parseEventTime("not-a-date"));
    }

    // verifies setDate() eagerly triggers parseEventTime, so getEventTimeMillis()
    // returns the correct value without re-parsing
    @Test
    void setDate_populatesEventTimeMillis() {
        TelemetryEvent event = new TelemetryEvent();
        event.setDate("2023-09-03T13:05:12.003Z");
        assertEquals(1693746312003L, event.getEventTimeMillis());
        assertEquals("2023-09-03T13:05:12.003Z", event.getDate());
    }
}
