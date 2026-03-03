package com.polimi.f1.events;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

// jackson-mapped pojo representing a single telemetry sample from the kafka f1-telemetry topic.
// field names match the fastf1 python producer's json keys (PascalCase via @JsonProperty).
// the "Date" field is parsed on set to derive eventTimeMillis for flink's event-time watermarks.
// ignoreUnknown = true lets us safely evolve the producer schema without breaking deserialization.
@JsonIgnoreProperties(ignoreUnknown = true)
public class TelemetryEvent {

    // car telemetry
    private String date;         // iso-8601 timestamp from the fastf1 session, ex: "2023-09-03T13:05:12.003"
    private double sessionTime;  // elapsed seconds since session start
    private int speed;           // km/h
    private int rpm;
    private int throttle;        // 0-100 (percentage)
    private int brake;           // 0 or 1 (binary in fastf1 car data)
    private int nGear;           // 0 = neutral, 1-8
    private int drs;             // 0-14, values 10-14 indicate drs open/activating

    // gps position (track coordinates, not lat/lon)
    private double x;
    private double y;
    private double z;

    private String driver;       // three-letter abbreviation, ex: "VER", "LEC"
    private String trackStatus;  // enriched by broadcast state, fia code: "1"=green, "4"=sc, "6"=vsc
    private long eventTimeMillis; // derived from date — used by flink for event-time semantics

    public TelemetryEvent() {}

    @JsonProperty("Date")
    public String getDate() {
        return date;
    }

    // eagerly parses event time on deserialization so the watermark assigner
    // can call getEventTimeMillis() without re-parsing every time
    @JsonProperty("Date")
    public void setDate(String date) {
        this.date = date;
        this.eventTimeMillis = parseEventTime(date);
    }

    @JsonProperty("SessionTime")
    public double getSessionTime() { return sessionTime; }
    @JsonProperty("SessionTime")
    public void setSessionTime(double sessionTime) { this.sessionTime = sessionTime; }

    @JsonProperty("Speed")
    public int getSpeed() { return speed; }
    @JsonProperty("Speed")
    public void setSpeed(int speed) { this.speed = speed; }

    @JsonProperty("RPM")
    public int getRpm() { return rpm; }
    @JsonProperty("RPM")
    public void setRpm(int rpm) { this.rpm = rpm; }

    @JsonProperty("Throttle")
    public int getThrottle() { return throttle; }
    @JsonProperty("Throttle")
    public void setThrottle(int throttle) { this.throttle = throttle; }

    @JsonProperty("Brake")
    public int getBrake() { return brake; }
    @JsonProperty("Brake")
    public void setBrake(int brake) { this.brake = brake; }

    @JsonProperty("nGear")
    public int getNGear() { return nGear; }
    @JsonProperty("nGear")
    public void setNGear(int nGear) { this.nGear = nGear; }

    @JsonProperty("DRS")
    public int getDrs() { return drs; }
    @JsonProperty("DRS")
    public void setDrs(int drs) { this.drs = drs; }

    @JsonProperty("X")
    public double getX() { return x; }
    @JsonProperty("X")
    public void setX(double x) { this.x = x; }

    @JsonProperty("Y")
    public double getY() { return y; }
    @JsonProperty("Y")
    public void setY(double y) { this.y = y; }

    @JsonProperty("Z")
    public double getZ() { return z; }
    @JsonProperty("Z")
    public void setZ(double z) { this.z = z; }

    @JsonProperty("Driver")
    public String getDriver() { return driver; }
    @JsonProperty("Driver")
    public void setDriver(String driver) { this.driver = driver; }

    @JsonIgnore
    public String getTrackStatus() { return trackStatus; }
    @JsonIgnore
    public void setTrackStatus(String trackStatus) { this.trackStatus = trackStatus; }

    @JsonIgnore
    public long getEventTimeMillis() { return eventTimeMillis; }
    @JsonIgnore
    public void setEventTimeMillis(long eventTimeMillis) { this.eventTimeMillis = eventTimeMillis; }

    // converts the iso-8601 date string to epoch millis for flink watermarks.
    // tries Instant.parse first (with timezone, ex: "2023-09-03T13:05:12.003Z"),
    // falls back to LocalDateTime (no timezone suffix) assuming UTC.
    // ex: "2023-09-03T13:05:12.003" -> 1693746312003
    static long parseEventTime(String isoDate) {
        if (isoDate == null || isoDate.isEmpty()) {
            return 0L;
        }
        try {
            return Instant.parse(isoDate).toEpochMilli();
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(isoDate)
                        .toInstant(ZoneOffset.UTC)
                        .toEpochMilli();
            } catch (DateTimeParseException e2) {
                return 0L;
            }
        }
    }

    @Override
    public String toString() {
        return "TelemetryEvent{" +
                "driver='" + driver + '\'' +
                ", date='" + date + '\'' +
                ", speed=" + speed +
                ", throttle=" + throttle +
                ", brake=" + brake +
                ", nGear=" + nGear +
                ", drs=" + drs +
                '}';
    }
}
