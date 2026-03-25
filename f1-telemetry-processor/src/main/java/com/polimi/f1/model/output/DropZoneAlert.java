package com.polimi.f1.model.output;

import com.polimi.f1.model.TrackStatusCodes;

// drop zone analysis: bridges the net race (positional rivals) and physical race
// (cars physically surrounding a driver after a pit stop).
//
// the net race is who you compete with for final classification. the physical race
// is where you actually end up on track after losing ~22s in the pit lane.
// a pit stop that gains a net position can still fail if the driver emerges in
// bad physical traffic (fast car on fresh tires = dirty air trap).
//
// for each driver per lap, answers: "if i pit now, where do i physically emerge,
// and is the traffic favorable?"
//
// ex: VER at P2, pitLoss=22s. cumulative gaps behind: P3=3.0s, P4=4.5s, P5=6.5s, P6=22.5s.
//     VER pits -> emerges behind P5 (STR) with 15.5s gap consumed, 6.5s remaining.
//     physicalCarAhead=STR on HARD tyreLife=25 (slow, easy pass).
//     netRival=HAM (P1, the undercut target in the classification race).
public class DropZoneAlert {

    public static final String CSV_HEADER
            = "driver,lapNumber,currentPosition,emergencePosition,positionsLost,"
            + "netRival,physicalCarAhead,gapToPhysicalCar,physicalCarCompound,"
            + "physicalCarTyreLife,trackStatus,pitLoss";

    private String driver;
    private int lapNumber;
    private int currentPosition;
    private int emergencePosition;
    private int positionsLost;
    private String netRival;
    private String physicalCarAhead;
    private double gapToPhysicalCar;
    private String physicalCarCompound;
    private int physicalCarTyreLife;
    private String trackStatus;
    private double pitLoss;

    public DropZoneAlert() {
    }

    public DropZoneAlert(String driver, int lapNumber, int currentPosition,
            int emergencePosition, int positionsLost, String netRival,
            String physicalCarAhead, double gapToPhysicalCar,
            String physicalCarCompound, int physicalCarTyreLife,
            String trackStatus, double pitLoss) {
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.currentPosition = currentPosition;
        this.emergencePosition = emergencePosition;
        this.positionsLost = positionsLost;
        this.netRival = netRival;
        this.physicalCarAhead = physicalCarAhead;
        this.gapToPhysicalCar = gapToPhysicalCar;
        this.physicalCarCompound = physicalCarCompound;
        this.physicalCarTyreLife = physicalCarTyreLife;
        this.trackStatus = trackStatus;
        this.pitLoss = pitLoss;
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

    public int getCurrentPosition() {
        return currentPosition;
    }

    public void setCurrentPosition(int currentPosition) {
        this.currentPosition = currentPosition;
    }

    public int getEmergencePosition() {
        return emergencePosition;
    }

    public void setEmergencePosition(int emergencePosition) {
        this.emergencePosition = emergencePosition;
    }

    public int getPositionsLost() {
        return positionsLost;
    }

    public void setPositionsLost(int positionsLost) {
        this.positionsLost = positionsLost;
    }

    public String getNetRival() {
        return netRival;
    }

    public void setNetRival(String netRival) {
        this.netRival = netRival;
    }

    public String getPhysicalCarAhead() {
        return physicalCarAhead;
    }

    public void setPhysicalCarAhead(String physicalCarAhead) {
        this.physicalCarAhead = physicalCarAhead;
    }

    public double getGapToPhysicalCar() {
        return gapToPhysicalCar;
    }

    public void setGapToPhysicalCar(double gapToPhysicalCar) {
        this.gapToPhysicalCar = gapToPhysicalCar;
    }

    public String getPhysicalCarCompound() {
        return physicalCarCompound;
    }

    public void setPhysicalCarCompound(String physicalCarCompound) {
        this.physicalCarCompound = physicalCarCompound;
    }

    public int getPhysicalCarTyreLife() {
        return physicalCarTyreLife;
    }

    public void setPhysicalCarTyreLife(int physicalCarTyreLife) {
        this.physicalCarTyreLife = physicalCarTyreLife;
    }

    public String getTrackStatus() {
        return trackStatus;
    }

    public void setTrackStatus(String trackStatus) {
        this.trackStatus = trackStatus;
    }

    public double getPitLoss() {
        return pitLoss;
    }

    public void setPitLoss(double pitLoss) {
        this.pitLoss = pitLoss;
    }

    private static String safe(String value) {
        return value != null ? value : "";
    }

    // ex: VER,25,2,7,5,HAM,STR,6.5,HARD,25,1,22.0
    // null string fields are emitted as empty cells to keep csv column alignment stable
    public String toCsvRow() {
        return String.join(",",
                safe(driver),
                String.valueOf(lapNumber),
                String.valueOf(currentPosition),
                String.valueOf(emergencePosition),
                String.valueOf(positionsLost),
                safe(netRival),
                safe(physicalCarAhead),
                String.format("%.3f", gapToPhysicalCar),
                safe(physicalCarCompound),
                String.valueOf(physicalCarTyreLife),
                TrackStatusCodes.normalizeOrGreen(trackStatus),
                String.format("%.1f", pitLoss)
        );
    }

    @Override
    public String toString() {
        return String.format(
                "DROP ZONE | %s P%d -> P%d (-%d) | Net rival: %s | "
                + "Emerges behind: %s (%s L%d) gap=%.1fs | PitLoss=%.1fs (status: %s)",
                driver, currentPosition, emergencePosition, positionsLost,
                netRival != null ? netRival : "P1",
                physicalCarAhead != null ? physicalCarAhead : "?",
                physicalCarCompound != null ? physicalCarCompound : "?",
                physicalCarTyreLife,
                gapToPhysicalCar, pitLoss,
                trackStatus);
    }
}
