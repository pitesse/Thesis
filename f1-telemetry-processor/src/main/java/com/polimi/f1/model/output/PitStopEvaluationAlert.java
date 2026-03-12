package com.polimi.f1.model.output;

// classification of a completed pit stop as success or failure.
// emitted after collecting 3 post-pit laps to evaluate time gap changes vs the net rival.
// enriched with context features (track status, tyre age, gap) for ml training.
// ex csv: VER,15,2.500,1.200,HARD,SUCCESS_UNDERCUT,1,24,4.832,Italian Grand Prix,LEC
public class PitStopEvaluationAlert {

    public static final String CSV_HEADER
            = "driver,pitLapNumber,prePitGapToRival,postPitGapToRival,compound,result,"
            + "trackStatusAtPit,tyreAgeAtPit,gapToCarAheadAtPit,race,netRival";

    public enum Result {
        SUCCESS_UNDERCUT, // gained time on rival (effective undercut)
        SUCCESS_DEFEND, // maintained time gap to rival (defended successfully)
        FAILURE_LOST_POSITION   // lost time to rival
    }

    private String driver;
    private int pitLapNumber;
    private Double prePitGapToRival;      // directed gap at pit entry: positive = behind rival, negative = ahead
    private Double postPitGapToRival;     // directed gap 3 laps after pit, same sign convention
    private String compound;              // tire compound after pit stop
    private Result result;
    private String trackStatusAtPit;      // track status code at pit entry, ex: "1" (green), "4" (sc)
    private int tyreAgeAtPit;             // laps completed on old tire set at pit entry
    private Double gapToCarAheadAtPit;    // gap in seconds to the car ahead when pitting
    private String race;                  // grand prix name, ex: "Italian Grand Prix"
    private String netRival;               // driver compared against for classification, ex: "LEC"

    public PitStopEvaluationAlert() {
    }

    public PitStopEvaluationAlert(String driver, int pitLapNumber, Double prePitGapToRival,
            Double postPitGapToRival, String compound, Result result,
            String trackStatusAtPit, int tyreAgeAtPit,
            Double gapToCarAheadAtPit, String race, String netRival) {
        this.driver = driver;
        this.pitLapNumber = pitLapNumber;
        this.prePitGapToRival = prePitGapToRival;
        this.postPitGapToRival = postPitGapToRival;
        this.compound = compound;
        this.result = result;
        this.trackStatusAtPit = trackStatusAtPit;
        this.tyreAgeAtPit = tyreAgeAtPit;
        this.gapToCarAheadAtPit = gapToCarAheadAtPit;
        this.race = race;
        this.netRival = netRival;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public int getPitLapNumber() {
        return pitLapNumber;
    }

    public void setPitLapNumber(int pitLapNumber) {
        this.pitLapNumber = pitLapNumber;
    }

    public Double getPrePitGapToRival() {
        return prePitGapToRival;
    }

    public void setPrePitGapToRival(Double prePitGapToRival) {
        this.prePitGapToRival = prePitGapToRival;
    }

    public Double getPostPitGapToRival() {
        return postPitGapToRival;
    }

    public void setPostPitGapToRival(Double postPitGapToRival) {
        this.postPitGapToRival = postPitGapToRival;
    }

    public String getCompound() {
        return compound;
    }

    public void setCompound(String compound) {
        this.compound = compound;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
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

    public String getNetRival() {
        return netRival;
    }

    public void setNetRival(String netRival) {
        this.netRival = netRival;
    }

    // ml-ready csv row, ex: VER,15,2.500,1.200,HARD,SUCCESS_UNDERCUT,1,24,4.832,Italian Grand Prix,LEC
    public String toCsvRow() {
        return String.join(",",
                driver,
                String.valueOf(pitLapNumber),
                prePitGapToRival != null ? String.format("%.3f", prePitGapToRival) : "",
                postPitGapToRival != null ? String.format("%.3f", postPitGapToRival) : "",
                compound,
                result.name(),
                trackStatusAtPit != null ? trackStatusAtPit : "",
                String.valueOf(tyreAgeAtPit),
                gapToCarAheadAtPit != null ? String.format("%.3f", gapToCarAheadAtPit) : "",
                race != null ? race : "",
                netRival != null ? netRival : ""
        );
    }

    @Override
    public String toString() {
        return String.format(
                "PIT EVAL | Driver: %s | Lap: %d | PreGap: %s | PostGap: %s | %s | Result: %s"
                + " | Track: %s | TyreAge: %d | Gap: %s | Race: %s | Rival: %s",
                driver, pitLapNumber,
                prePitGapToRival != null ? String.format("%.3fs", prePitGapToRival) : "N/A",
                postPitGapToRival != null ? String.format("%.3fs", postPitGapToRival) : "N/A",
                compound, result,
                trackStatusAtPit, tyreAgeAtPit,
                gapToCarAheadAtPit != null ? String.format("%.3f", gapToCarAheadAtPit) : "N/A",
                race != null ? race : "N/A",
                netRival != null ? netRival : "N/A");
    }
}
