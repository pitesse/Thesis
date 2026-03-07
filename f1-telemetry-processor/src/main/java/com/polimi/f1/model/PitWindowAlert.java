package com.polimi.f1.model;

// emitted when a driver's gap to the car behind is large enough to pit without losing position
// (SAFE_PIT) or when the car ahead is close enough that an undercut could gain a position
// (UNDERCUT_OPPORTUNITY). the threshold adapts dynamically based on track status and
// the circuit-specific pit loss times embedded by the python producer.
public class PitWindowAlert {

    public static final String CSV_HEADER
            = "driver,lapNumber,alertType,gapBehind,gapAhead,trackStatus,threshold";

    private String driver;
    private int lapNumber;
    private String alertType;         // "SAFE_PIT" or "UNDERCUT_OPPORTUNITY"
    private double gapBehind;         // seconds to the car behind
    private Double gapAhead;          // seconds to the car ahead, null when P1
    private String trackStatus;       // fia code at the time of evaluation
    private double threshold;         // dynamic threshold based on track status (seconds)

    public PitWindowAlert() {
    }

    public PitWindowAlert(String driver, int lapNumber, String alertType,
            double gapBehind, Double gapAhead, String trackStatus, double threshold) {
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.alertType = alertType;
        this.gapBehind = gapBehind;
        this.gapAhead = gapAhead;
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

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public double getGapBehind() {
        return gapBehind;
    }

    public void setGapBehind(double gapBehind) {
        this.gapBehind = gapBehind;
    }

    public Double getGapAhead() {
        return gapAhead;
    }

    public void setGapAhead(Double gapAhead) {
        this.gapAhead = gapAhead;
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

    // ex: VER,25,SAFE_PIT,28.5,3.2,1,22.0
    public String toCsvRow() {
        return String.join(",",
                driver,
                String.valueOf(lapNumber),
                alertType != null ? alertType : "",
                String.format("%.3f", gapBehind),
                gapAhead != null ? String.format("%.3f", gapAhead) : "",
                trackStatus != null ? trackStatus : "",
                String.format("%.1f", threshold)
        );
    }

    @Override
    public String toString() {
        return String.format(
                "%s | Driver: %s | Lap: %d | Gap behind: %.1fs | Gap ahead: %s | Threshold: %.1fs (status: %s)",
                alertType != null ? alertType : "PIT WINDOW",
                driver, lapNumber, gapBehind,
                gapAhead != null ? String.format("%.1fs", gapAhead) : "N/A",
                threshold, trackStatus);
    }
}
