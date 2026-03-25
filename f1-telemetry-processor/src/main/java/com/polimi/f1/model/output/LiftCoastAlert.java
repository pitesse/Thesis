package com.polimi.f1.model.output;

import com.polimi.f1.model.TrackStatusCodes;

// emitted when cep detects a lift & coast maneuver: the driver releases full throttle,
// coasts briefly with no pedal input, then applies brakes later than a normal braking point.
// the brakeDate serves as the dedup key, multiple cep matches from consecutive throttle=100
// samples converge on the same lift+brake event.
// speed and gear at the point of full throttle provide context for ml analysis
// (lift & coast typically happens on long straights at high speed in top gear).
public class LiftCoastAlert {

    public static final String CSV_HEADER
            = "driver,fullThrottleDate,liftDate,brakeDate,trackStatus,speed,gear";

    private String driver;
    private String fullThrottleDate;   // iso timestamp of the last full-throttle sample
    private String liftDate;           // iso timestamp of the throttle=0 coast sample
    private String brakeDate;          // iso timestamp of the brake application
    private String trackStatus;        // fia track status at time of detection, ex: "1"=green
    private int speed;                 // km/h at the full-throttle point
    private int gear;                  // gear at the full-throttle point (7 or 8 on straights)

    public LiftCoastAlert() {
    }

    public LiftCoastAlert(String driver, String fullThrottleDate, String liftDate,
            String brakeDate, String trackStatus, int speed, int gear) {
        this.driver = driver;
        this.fullThrottleDate = fullThrottleDate;
        this.liftDate = liftDate;
        this.brakeDate = brakeDate;
        this.trackStatus = trackStatus;
        this.speed = speed;
        this.gear = gear;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getFullThrottleDate() {
        return fullThrottleDate;
    }

    public void setFullThrottleDate(String fullThrottleDate) {
        this.fullThrottleDate = fullThrottleDate;
    }

    public String getLiftDate() {
        return liftDate;
    }

    public void setLiftDate(String liftDate) {
        this.liftDate = liftDate;
    }

    public String getBrakeDate() {
        return brakeDate;
    }

    public void setBrakeDate(String brakeDate) {
        this.brakeDate = brakeDate;
    }

    public String getTrackStatus() {
        return trackStatus;
    }

    public void setTrackStatus(String trackStatus) {
        this.trackStatus = trackStatus;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public int getGear() {
        return gear;
    }

    public void setGear(int gear) {
        this.gear = gear;
    }

    // ex: VER,2023-09-03T13:05:12.003,2023-09-03T13:05:13.003,2023-09-03T13:05:14.003,1,342,8
    // timestamp and driver fields are null safe, speed and gear are primitive values
    public String toCsvRow() {
        return String.join(",",
                driver != null ? driver : "",
                fullThrottleDate != null ? fullThrottleDate : "",
                liftDate != null ? liftDate : "",
                brakeDate != null ? brakeDate : "",
                TrackStatusCodes.normalizeOrGreen(trackStatus),
                String.valueOf(speed),
                String.valueOf(gear)
        );
    }

    @Override
    public String toString() {
        return String.format(
                "LIFT & COAST | Driver: %s | Throttle@%s -> Lift@%s -> Brake@%s | Status: %s | %dkm/h G%d",
                driver, fullThrottleDate, liftDate, brakeDate,
                trackStatus != null ? trackStatus : "unknown",
                speed, gear);
    }
}
