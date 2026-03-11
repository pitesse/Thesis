package com.polimi.f1.model.output;

// emitted when a driver's rolling lap time average degrades beyond a threshold
// relative to their stint best. indicates tire performance cliff.
// ex csv: VER,25,SOFT,18,83.421,81.200,2.221
public class TireDropAlert {

    public static final String CSV_HEADER
            = "driver,lapNumber,compound,tyreLife,rollingAvg,stintBest,delta";

    private String driver;
    private int lapNumber;
    private String compound;
    private int tyreLife;
    private double rollingAvg;    // rolling average of last 3 laps (seconds)
    private double stintBest;     // best lap time in current stint (seconds)
    private double delta;         // rollingAvg - stintBest (seconds)

    public TireDropAlert() {
    }

    public TireDropAlert(String driver, int lapNumber, String compound,
            int tyreLife, double rollingAvg, double stintBest, double delta) {
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.compound = compound;
        this.tyreLife = tyreLife;
        this.rollingAvg = rollingAvg;
        this.stintBest = stintBest;
        this.delta = delta;
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

    public String getCompound() {
        return compound;
    }

    public void setCompound(String compound) {
        this.compound = compound;
    }

    public int getTyreLife() {
        return tyreLife;
    }

    public void setTyreLife(int tyreLife) {
        this.tyreLife = tyreLife;
    }

    public double getRollingAvg() {
        return rollingAvg;
    }

    public void setRollingAvg(double rollingAvg) {
        this.rollingAvg = rollingAvg;
    }

    public double getStintBest() {
        return stintBest;
    }

    public void setStintBest(double stintBest) {
        this.stintBest = stintBest;
    }

    public double getDelta() {
        return delta;
    }

    public void setDelta(double delta) {
        this.delta = delta;
    }

    // ml-ready csv row, ex: VER,25,SOFT,18,83.421,81.200,2.221
    public String toCsvRow() {
        return String.join(",",
                driver,
                String.valueOf(lapNumber),
                compound,
                String.valueOf(tyreLife),
                String.format("%.3f", rollingAvg),
                String.format("%.3f", stintBest),
                String.format("%.3f", delta)
        );
    }

    @Override
    public String toString() {
        return String.format(
                "TIRE DROP | Driver: %s | Lap: %d | %s (life: %d) | avg: %.3fs | best: %.3fs | delta: +%.3fs",
                driver, lapNumber, compound, tyreLife, rollingAvg, stintBest, delta);
    }
}
