package com.polimi.f1.model;

// classification of a completed pit stop as success or failure.
// emitted after collecting 3 post-pit laps to evaluate position changes.
// enriched with context features (track status, tyre age, gap) for ml training.
// ex csv: VER,15,2,2,HARD,SUCCESS_DEFEND,1,24,4.832
public class PitStopEvaluationAlert {

    public static final String CSV_HEADER =
            "driver,pitLapNumber,prePitPosition,postPitPosition,compound,result,"
            + "trackStatusAtPit,tyreAgeAtPit,gapToCarAheadAtPit";

    public enum Result {
        SUCCESS_UNDERCUT,       // gained positions (effective undercut)
        SUCCESS_DEFEND,         // maintained position (defended successfully)
        FAILURE_LOST_POSITION   // lost positions
    }

    private String driver;
    private int pitLapNumber;
    private int prePitPosition;
    private int postPitPosition;
    private String compound;              // tire compound after pit stop
    private Result result;
    private String trackStatusAtPit;      // track status code at pit entry, ex: "1" (green), "4" (sc)
    private int tyreAgeAtPit;             // laps completed on old tire set at pit entry
    private Double gapToCarAheadAtPit;    // gap in seconds to the car ahead when pitting

    public PitStopEvaluationAlert() {}

    public PitStopEvaluationAlert(String driver, int pitLapNumber, int prePitPosition,
                             int postPitPosition, String compound, Result result,
                             String trackStatusAtPit, int tyreAgeAtPit,
                             Double gapToCarAheadAtPit) {
        this.driver = driver;
        this.pitLapNumber = pitLapNumber;
        this.prePitPosition = prePitPosition;
        this.postPitPosition = postPitPosition;
        this.compound = compound;
        this.result = result;
        this.trackStatusAtPit = trackStatusAtPit;
        this.tyreAgeAtPit = tyreAgeAtPit;
        this.gapToCarAheadAtPit = gapToCarAheadAtPit;
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

    public String getTrackStatusAtPit() { return trackStatusAtPit; }
    public void setTrackStatusAtPit(String trackStatusAtPit) { this.trackStatusAtPit = trackStatusAtPit; }

    public int getTyreAgeAtPit() { return tyreAgeAtPit; }
    public void setTyreAgeAtPit(int tyreAgeAtPit) { this.tyreAgeAtPit = tyreAgeAtPit; }

    public Double getGapToCarAheadAtPit() { return gapToCarAheadAtPit; }
    public void setGapToCarAheadAtPit(Double gapToCarAheadAtPit) { this.gapToCarAheadAtPit = gapToCarAheadAtPit; }

    // csv row, ex: VER,15,2,2,HARD,SUCCESS_DEFEND,1,24,4.832 
    public String toCsvRow() {
        return String.join(",",
                driver,
                String.valueOf(pitLapNumber),
                String.valueOf(prePitPosition),
                String.valueOf(postPitPosition),
                compound,
                result.name(),
                trackStatusAtPit != null ? trackStatusAtPit : "",
                String.valueOf(tyreAgeAtPit),
                gapToCarAheadAtPit != null ? String.format("%.3f", gapToCarAheadAtPit) : ""
        );
    }

    @Override
    public String toString() {
        return String.format(
                "PIT EVAL | Driver: %s | Lap: %d | Pre: P%d | Post: P%d | %s | Result: %s"
                + " | Track: %s | TyreAge: %d | Gap: %s",
                driver, pitLapNumber, prePitPosition, postPitPosition, compound, result,
                trackStatusAtPit, tyreAgeAtPit,
                gapToCarAheadAtPit != null ? String.format("%.3f", gapToCarAheadAtPit) : "N/A");
    }
}
