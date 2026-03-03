package com.polimi.f1.model;

// classification of a completed pit stop as success or failure.
// emitted after collecting 3 post-pit laps to evaluate position changes.
public class PitStopEvaluationAlert {

    public enum Result {
        SUCCESS_UNDERCUT,       // gained positions (effective undercut)
        SUCCESS_DEFEND,         // maintained position (defended successfully)
        FAILURE_LOST_POSITION   // lost positions
    }

    private String driver;
    private int pitLapNumber;
    private int prePitPosition;
    private int postPitPosition;
    private String compound;       // tire compound after pit stop
    private Result result;

    public PitStopEvaluationAlert() {}

    public PitStopEvaluationAlert(String driver, int pitLapNumber, int prePitPosition,
                             int postPitPosition, String compound, Result result) {
        this.driver = driver;
        this.pitLapNumber = pitLapNumber;
        this.prePitPosition = prePitPosition;
        this.postPitPosition = postPitPosition;
        this.compound = compound;
        this.result = result;
    }

    public String getDriver() { return driver; }
    public void setDriver(String driver) { this.driver = driver; }

    public int getPitLapNumber() { return pitLapNumber; }
    public void setPitLapNumber(int pitLapNumber) { this.pitLapNumber = pitLapNumber; }

    public int getPrePitPosition() { return prePitPosition; }
    public void setPrePitPosition(int prePitPosition) { this.prePitPosition = prePitPosition; }

    public int getPostPitPosition() { return postPitPosition; }
    public void setPostPitPosition(int postPitPosition) { this.postPitPosition = postPitPosition; }

    public String getCompound() { return compound; }
    public void setCompound(String compound) { this.compound = compound; }

    public Result getResult() { return result; }
    public void setResult(Result result) { this.result = result; }

    @Override
    public String toString() {
        return String.format(
                "PIT EVAL | Driver: %s | Lap: %d | Pre: P%d | Post: P%d | %s | Result: %s",
                driver, pitLapNumber, prePitPosition, postPitPosition, compound, result);
    }
}
