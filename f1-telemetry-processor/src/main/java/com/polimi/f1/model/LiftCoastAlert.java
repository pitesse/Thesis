package com.polimi.f1.model;

// emitted when cep detects a lift & coast maneuver: the driver releases full throttle,
// coasts briefly with no pedal input, then applies brakes later than a normal braking point.
// the brakeDate serves as the dedup key, multiple cep matches from consecutive throttle=100
// samples converge on the same lift+brake event.
public class LiftCoastAlert {

    private String driver;
    private String fullThrottleDate;   // iso timestamp of the last full-throttle sample
    private String liftDate;           // iso timestamp of the throttle=0 coast sample
    private String brakeDate;          // iso timestamp of the brake application
    private String trackStatus;        // fia track status at time of detection, ex: "1"=green

    public LiftCoastAlert() {}

    public LiftCoastAlert(String driver, String fullThrottleDate, String liftDate,
                           String brakeDate, String trackStatus) {
        this.driver = driver;
        this.fullThrottleDate = fullThrottleDate;
        this.liftDate = liftDate;
        this.brakeDate = brakeDate;
        this.trackStatus = trackStatus;
    }

    public String getDriver() { return driver; }
    public void setDriver(String driver) { this.driver = driver; }

    public String getFullThrottleDate() { return fullThrottleDate; }
    public void setFullThrottleDate(String fullThrottleDate) { this.fullThrottleDate = fullThrottleDate; }

    public String getLiftDate() { return liftDate; }
    public void setLiftDate(String liftDate) { this.liftDate = liftDate; }

    public String getBrakeDate() { return brakeDate; }
    public void setBrakeDate(String brakeDate) { this.brakeDate = brakeDate; }

    public String getTrackStatus() { return trackStatus; }
    public void setTrackStatus(String trackStatus) { this.trackStatus = trackStatus; }

    @Override
    public String toString() {
        return String.format(
                "LIFT & COAST | Driver: %s | Throttle@%s -> Lift@%s -> Brake@%s | Status: %s",
                driver, fullThrottleDate, liftDate, brakeDate,
                trackStatus != null ? trackStatus : "unknown");
    }
}
