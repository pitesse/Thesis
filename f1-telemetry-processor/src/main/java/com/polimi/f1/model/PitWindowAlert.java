package com.polimi.f1.model;

// emitted when a driver's gap to the car behind is large enough to pit without losing position.
// the threshold adapts dynamically based on track status (green/vsc/sc).
public class PitWindowAlert {

    private String driver;
    private int lapNumber;
    private double gapBehind;     // seconds to the car behind
    private String trackStatus;   // fia code at the time of evaluation
    private double threshold;     // dynamic threshold based on track status (seconds)

    public PitWindowAlert() {
    }

    public PitWindowAlert(String driver, int lapNumber, double gapBehind,
            String trackStatus, double threshold) {
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.gapBehind = gapBehind;
        this.trackStatus = trackStatus;
        this.threshold = threshold;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public int getLapNumber() {
        return lapNumber;
    }

    public void setLapNumber(int lapNumber) {
        this.lapNumber = lapNumber;
    }

    public double getGapBehind() {
        return gapBehind;
    }

    public void setGapBehind(double gapBehind) {
        this.gapBehind = gapBehind;
    }

    public String getTrackStatus() {
        return trackStatus;
    }

    public void setTrackStatus(String trackStatus) {
        this.trackStatus = trackStatus;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return String.format(
                "PIT WINDOW OPEN | Driver: %s | Lap: %d | Gap behind: %.1fs | Threshold: %.1fs (status: %s)",
                driver, lapNumber, gapBehind, threshold, trackStatus);
    }
}
