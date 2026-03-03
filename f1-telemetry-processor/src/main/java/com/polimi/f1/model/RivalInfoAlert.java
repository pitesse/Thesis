package com.polimi.f1.model;

// output of the rival identification window function.
// for each driver on each lap, identifies who is directly ahead and behind
// along with the time gap to each.
public class RivalInfoAlert {

    private String driver;
    private String driverAhead;    // null if leading the tracked group
    private String driverBehind;   // null if last in the tracked group
    private Double gapAhead;       // seconds, null if leading
    private Double gapBehind;      // seconds, null if last
    private int lapNumber;
    private int position;

    public RivalInfoAlert() {}

    public RivalInfoAlert(String driver, String driverAhead, String driverBehind,
                     Double gapAhead, Double gapBehind, int lapNumber, int position) {
        this.driver = driver;
        this.driverAhead = driverAhead;
        this.driverBehind = driverBehind;
        this.gapAhead = gapAhead;
        this.gapBehind = gapBehind;
        this.lapNumber = lapNumber;
        this.position = position;
    }

    public String getDriver() { return driver; }
    public void setDriver(String driver) { this.driver = driver; }

    public String getDriverAhead() { return driverAhead; }
    public void setDriverAhead(String driverAhead) { this.driverAhead = driverAhead; }

    public String getDriverBehind() { return driverBehind; }
    public void setDriverBehind(String driverBehind) { this.driverBehind = driverBehind; }

    public Double getGapAhead() { return gapAhead; }
    public void setGapAhead(Double gapAhead) { this.gapAhead = gapAhead; }

    public Double getGapBehind() { return gapBehind; }
    public void setGapBehind(Double gapBehind) { this.gapBehind = gapBehind; }

    public int getLapNumber() { return lapNumber; }
    public void setLapNumber(int lapNumber) { this.lapNumber = lapNumber; }

    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }

    @Override
    public String toString() {
        return "RivalInfoAlert{" +
                "driver='" + driver + '\'' +
                ", pos=" + position +
                ", ahead='" + driverAhead + '\'' +
                ", behind='" + driverBehind + '\'' +
                ", gapAhead=" + gapAhead +
                ", gapBehind=" + gapBehind +
                ", lap=" + lapNumber +
                '}';
    }
}
