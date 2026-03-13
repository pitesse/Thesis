package com.polimi.f1.state.groundtruth;

import java.io.Serializable;

/**
 * state for tracking a pending pit cycle evaluation. contains all context
 * needed to evaluate a pit stop including rival clusters, timing data, track
 * conditions, and resolution tracking through the state machine.
 *
 * follows flink pojo serialization requirements with private fields and public
 * accessors. serialization compatibility maintained via serialVersionUID for
 * state recovery.
 */
public class PitCycle implements Serializable {

    private static final long serialVersionUID = 3L;

    // basic pit cycle info
    private String driver;
    private int pitLap;
    private int stintBeforePit;
    private String compoundAfterPit;
    private String trackStatusAtPit;
    private int tyreAgeAtPit;
    private double baselineLapTime;
    private int totalLaps;

    // rival cluster (cars ahead and behind at pit entry)
    private String rivalAhead;
    private String rivalBehind;
    private Double prePitGapAhead;
    private Double prePitGapBehind;
    private int rivalAheadStintAtPit;
    private int rivalBehindStintAtPit;

    // primary rival (ahead for most positions, behind for P1)
    private String primaryRival;
    private Double prePitGapToPrimary;
    private int primaryRivalStintAtPit;
    private boolean driverPittedFirst;

    // resolution tracking
    private CycleState state;
    private int settleLap;
    private int offsetTimeoutLap;
    private long safetyTimerTimestamp;
    private Double gapToCarAheadAtPit;
    private String race;

    /**
     * default no-argument constructor required for flink pojo serialization.
     */
    public PitCycle() {
    }

    // driver and pit timing accessors
    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public int getPitLap() {
        return pitLap;
    }

    public void setPitLap(int pitLap) {
        this.pitLap = pitLap;
    }

    public int getStintBeforePit() {
        return stintBeforePit;
    }

    public void setStintBeforePit(int stintBeforePit) {
        this.stintBeforePit = stintBeforePit;
    }

    public String getCompoundAfterPit() {
        return compoundAfterPit;
    }

    public void setCompoundAfterPit(String compoundAfterPit) {
        this.compoundAfterPit = compoundAfterPit;
    }

    public String getTrackStatusAtPit() {
        return trackStatusAtPit;
    }

    public void setTrackStatusAtPit(String trackStatusAtPit) {
        this.trackStatusAtPit = trackStatusAtPit;
    }

    public int getTyreAgeAtPit() {
        return tyreAgeAtPit;
    }

    public void setTyreAgeAtPit(int tyreAgeAtPit) {
        this.tyreAgeAtPit = tyreAgeAtPit;
    }

    public double getBaselineLapTime() {
        return baselineLapTime;
    }

    public void setBaselineLapTime(double baselineLapTime) {
        this.baselineLapTime = baselineLapTime;
    }

    public int getTotalLaps() {
        return totalLaps;
    }

    public void setTotalLaps(int totalLaps) {
        this.totalLaps = totalLaps;
    }

    // rival cluster accessors
    public String getRivalAhead() {
        return rivalAhead;
    }

    public void setRivalAhead(String rivalAhead) {
        this.rivalAhead = rivalAhead;
    }

    public String getRivalBehind() {
        return rivalBehind;
    }

    public void setRivalBehind(String rivalBehind) {
        this.rivalBehind = rivalBehind;
    }

    public Double getPrePitGapAhead() {
        return prePitGapAhead;
    }

    public void setPrePitGapAhead(Double prePitGapAhead) {
        this.prePitGapAhead = prePitGapAhead;
    }

    public Double getPrePitGapBehind() {
        return prePitGapBehind;
    }

    public void setPrePitGapBehind(Double prePitGapBehind) {
        this.prePitGapBehind = prePitGapBehind;
    }

    public int getRivalAheadStintAtPit() {
        return rivalAheadStintAtPit;
    }

    public void setRivalAheadStintAtPit(int rivalAheadStintAtPit) {
        this.rivalAheadStintAtPit = rivalAheadStintAtPit;
    }

    public int getRivalBehindStintAtPit() {
        return rivalBehindStintAtPit;
    }

    public void setRivalBehindStintAtPit(int rivalBehindStintAtPit) {
        this.rivalBehindStintAtPit = rivalBehindStintAtPit;
    }

    // primary rival accessors
    public String getPrimaryRival() {
        return primaryRival;
    }

    public void setPrimaryRival(String primaryRival) {
        this.primaryRival = primaryRival;
    }

    public Double getPrePitGapToPrimary() {
        return prePitGapToPrimary;
    }

    public void setPrePitGapToPrimary(Double prePitGapToPrimary) {
        this.prePitGapToPrimary = prePitGapToPrimary;
    }

    public int getPrimaryRivalStintAtPit() {
        return primaryRivalStintAtPit;
    }

    public void setPrimaryRivalStintAtPit(int primaryRivalStintAtPit) {
        this.primaryRivalStintAtPit = primaryRivalStintAtPit;
    }

    public boolean isDriverPittedFirst() {
        return driverPittedFirst;
    }

    public void setDriverPittedFirst(boolean driverPittedFirst) {
        this.driverPittedFirst = driverPittedFirst;
    }

    // resolution tracking accessors
    public CycleState getState() {
        return state;
    }

    public void setState(CycleState state) {
        this.state = state;
    }

    public int getSettleLap() {
        return settleLap;
    }

    public void setSettleLap(int settleLap) {
        this.settleLap = settleLap;
    }

    public int getOffsetTimeoutLap() {
        return offsetTimeoutLap;
    }

    public void setOffsetTimeoutLap(int offsetTimeoutLap) {
        this.offsetTimeoutLap = offsetTimeoutLap;
    }

    public long getSafetyTimerTimestamp() {
        return safetyTimerTimestamp;
    }

    public void setSafetyTimerTimestamp(long safetyTimerTimestamp) {
        this.safetyTimerTimestamp = safetyTimerTimestamp;
    }

    public Double getGapToCarAheadAtPit() {
        return gapToCarAheadAtPit;
    }

    public void setGapToCarAheadAtPit(Double gapToCarAheadAtPit) {
        this.gapToCarAheadAtPit = gapToCarAheadAtPit;
    }

    public String getRace() {
        return race;
    }

    public void setRace(String race) {
        this.race = race;
    }

    @Override
    public String toString() {
        return String.format(
                "PitCycle{driver='%s', lap=%d, state=%s, primaryRival='%s', race='%s'}",
                driver, pitLap, state, primaryRival, race);
    }
}
