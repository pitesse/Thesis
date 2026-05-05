package com.polimi.f1.model.output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.polimi.f1.model.input.LapEvent;

// canonical pit-timing truth row emitted from lap events when PitInTime is present.
// used by offline pipelines to build the pit_any_h2 target from raw FastF1 timing.
public class PitTimingEvent {

    private String race;
    private String driver;
    private int lapNumber;
    private String date;
    private Double pitInTime;
    private Double pitOutTime;

    public PitTimingEvent() {
    }

    public PitTimingEvent(
            String race,
            String driver,
            int lapNumber,
            String date,
            Double pitInTime,
            Double pitOutTime
    ) {
        this.race = race;
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.date = date;
        this.pitInTime = pitInTime;
        this.pitOutTime = pitOutTime;
    }

    public static PitTimingEvent fromLapEvent(LapEvent lap) {
        return new PitTimingEvent(
                lap.getRace(),
                lap.getDriver(),
                lap.getLapNumber(),
                lap.getDate(),
                lap.getPitInTime(),
                lap.getPitOutTime()
        );
    }

    @JsonProperty("race")
    public String getRace() {
        return race;
    }

    @JsonProperty("race")
    public void setRace(String race) {
        this.race = race;
    }

    @JsonProperty("driver")
    public String getDriver() {
        return driver;
    }

    @JsonProperty("driver")
    public void setDriver(String driver) {
        this.driver = driver;
    }

    @JsonProperty("lapNumber")
    public int getLapNumber() {
        return lapNumber;
    }

    @JsonProperty("lapNumber")
    public void setLapNumber(int lapNumber) {
        this.lapNumber = lapNumber;
    }

    @JsonProperty("date")
    public String getDate() {
        return date;
    }

    @JsonProperty("date")
    public void setDate(String date) {
        this.date = date;
    }

    @JsonProperty("pitInTime")
    public Double getPitInTime() {
        return pitInTime;
    }

    @JsonProperty("pitInTime")
    public void setPitInTime(Double pitInTime) {
        this.pitInTime = pitInTime;
    }

    @JsonProperty("pitOutTime")
    public Double getPitOutTime() {
        return pitOutTime;
    }

    @JsonProperty("pitOutTime")
    public void setPitOutTime(Double pitOutTime) {
        this.pitOutTime = pitOutTime;
    }

    @Override
    public String toString() {
        return "PitTimingEvent{" +
                "race='" + race + '\'' +
                ", driver='" + driver + '\'' +
                ", lapNumber=" + lapNumber +
                ", date='" + date + '\'' +
                ", pitInTime=" + pitInTime +
                ", pitOutTime=" + pitOutTime +
                '}';
    }
}
