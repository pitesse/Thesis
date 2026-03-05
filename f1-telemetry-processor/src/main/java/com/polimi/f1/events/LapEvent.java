package com.polimi.f1.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

// represents a completed lap from the f1-laps kafka topic.
// one event per driver per lap, emitted at LapStartDate (used as event time).
// contains timing, tire, and position data from fastf1's session.laps dataframe.
// nullable Double fields handle missing data, ex: pit in-laps often lack sector times.
@JsonIgnoreProperties(ignoreUnknown = true)
public class LapEvent {

    private String date;          // LapStartDate as iso-8601, used as event time
    private String driver;        // ex: "VER", "LEC"
    private int lapNumber;
    private Double lapTime;       // seconds, null on pit in/out laps
    private Double sector1Time;   // seconds
    private Double sector2Time;
    private Double sector3Time;
    private int stint;            // 1-based stint number, increments on pit stop
    private String compound;      // "SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"
    private int tyreLife;         // laps completed on current tire set
    private boolean freshTyre;
    private int position;         // race position at end of lap
    private Double pitInTime;     // session-relative seconds, null if no pit entry
    private Double pitOutTime;    // session-relative seconds, null if no pit exit
    private String trackStatus;   // status codes active during this lap, ex: "1" (green), "4" (sc)
    private Double gapToCarAhead; // seconds, computed from cumulative race time deltas
    private long eventTimeMillis;

    public LapEvent() {
    }

    @JsonProperty("Date")
    public String getDate() {
        return date;
    }

    @JsonProperty("Date")
    public void setDate(String date) {
        this.date = date;
        this.eventTimeMillis = TelemetryEvent.parseEventTime(date);
    }

    @JsonProperty("Driver")
    public String getDriver() {
        return driver;
    }

    @JsonProperty("Driver")
    public void setDriver(String driver) {
        this.driver = driver;
    }

    @JsonProperty("LapNumber")
    public int getLapNumber() {
        return lapNumber;
    }

    @JsonProperty("LapNumber")
    public void setLapNumber(int lapNumber) {
        this.lapNumber = lapNumber;
    }

    @JsonProperty("LapTime")
    public Double getLapTime() {
        return lapTime;
    }

    @JsonProperty("LapTime")
    public void setLapTime(Double lapTime) {
        this.lapTime = lapTime;
    }

    @JsonProperty("Sector1Time")
    public Double getSector1Time() {
        return sector1Time;
    }

    @JsonProperty("Sector1Time")
    public void setSector1Time(Double sector1Time) {
        this.sector1Time = sector1Time;
    }

    @JsonProperty("Sector2Time")
    public Double getSector2Time() {
        return sector2Time;
    }

    @JsonProperty("Sector2Time")
    public void setSector2Time(Double sector2Time) {
        this.sector2Time = sector2Time;
    }

    @JsonProperty("Sector3Time")
    public Double getSector3Time() {
        return sector3Time;
    }

    @JsonProperty("Sector3Time")
    public void setSector3Time(Double sector3Time) {
        this.sector3Time = sector3Time;
    }

    @JsonProperty("Stint")
    public int getStint() {
        return stint;
    }

    @JsonProperty("Stint")
    public void setStint(int stint) {
        this.stint = stint;
    }

    @JsonProperty("Compound")
    public String getCompound() {
        return compound;
    }

    @JsonProperty("Compound")
    public void setCompound(String compound) {
        this.compound = compound;
    }

    @JsonProperty("TyreLife")
    public int getTyreLife() {
        return tyreLife;
    }

    @JsonProperty("TyreLife")
    public void setTyreLife(int tyreLife) {
        this.tyreLife = tyreLife;
    }

    @JsonProperty("FreshTyre")
    public boolean isFreshTyre() {
        return freshTyre;
    }

    @JsonProperty("FreshTyre")
    public void setFreshTyre(boolean freshTyre) {
        this.freshTyre = freshTyre;
    }

    @JsonProperty("Position")
    public int getPosition() {
        return position;
    }

    @JsonProperty("Position")
    public void setPosition(int position) {
        this.position = position;
    }

    @JsonProperty("PitInTime")
    public Double getPitInTime() {
        return pitInTime;
    }

    @JsonProperty("PitInTime")
    public void setPitInTime(Double pitInTime) {
        this.pitInTime = pitInTime;
    }

    @JsonProperty("PitOutTime")
    public Double getPitOutTime() {
        return pitOutTime;
    }

    @JsonProperty("PitOutTime")
    public void setPitOutTime(Double pitOutTime) {
        this.pitOutTime = pitOutTime;
    }

    @JsonProperty("TrackStatus")
    public String getTrackStatus() {
        return trackStatus;
    }

    @JsonProperty("TrackStatus")
    public void setTrackStatus(String trackStatus) {
        this.trackStatus = trackStatus;
    }

    @JsonProperty("GapToCarAhead")
    public Double getGapToCarAhead() {
        return gapToCarAhead;
    }

    @JsonProperty("GapToCarAhead")
    public void setGapToCarAhead(Double gapToCarAhead) {
        this.gapToCarAhead = gapToCarAhead;
    }

    @JsonIgnore
    public long getEventTimeMillis() {
        return eventTimeMillis;
    }

    @JsonIgnore
    public void setEventTimeMillis(long eventTimeMillis) {
        this.eventTimeMillis = eventTimeMillis;
    }

    @Override
    public String toString() {
        return "LapEvent{"
                + "driver='" + driver + '\''
                + ", lap=" + lapNumber
                + ", time=" + lapTime
                + ", compound='" + compound + '\''
                + ", tyreLife=" + tyreLife
                + ", pos=" + position
                + ", gap=" + gapToCarAhead
                + '}';
    }
}
