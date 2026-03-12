package com.polimi.f1.model.output;

// classification of a completed pit cycle as success or failure.
// tracks rival clusters (car ahead + behind), detects offset strategies (1-stop vs 2-stop),
// and normalizes gap deltas as percentage of baseline lap time for track-agnostic scoring.
// enriched with context features (track status, tyre age, emergence traffic) for ml training.
// ex csv: VER,15,LEC,SAI,2.500,-1.200,1.200,HARD,SUCCESS_UNDERCUT,1,24,4.832,Italian Grand Prix,-1.5,82.3,true,false,RIVAL_PIT
public class PitStopEvaluationAlert {

    public static final String CSV_HEADER
            = "driver,pitLapNumber,rivalAhead,rivalBehind,prePitGapAhead,prePitGapBehind,"
            + "postPitGapToRival,compound,result,trackStatusAtPit,tyreAgeAtPit,"
            + "gapToCarAheadAtPit,race,gapDeltaPct,baselineLapTime,"
            + "driverPittedFirst,isOffsetStrategy,resolvedVia";

    public enum Result {
        SUCCESS_UNDERCUT, // gained time on rival by pitting first (effective undercut)
        SUCCESS_OVERCUT, // gained time by staying out while rival pitted first
        SUCCESS_DEFEND, // maintained gap to rival within tolerance band
        SUCCESS_FREE_STOP, // pitted under sc/vsc with minimal loss
        OFFSET_ADVANTAGE, // on different strategy (e.g. 1-stop vs 2-stop), gap improved
        OFFSET_DISADVANTAGE, // on different strategy, gap worsened
        FAILURE_PACE_DEFICIT, // lost ground to rival due to poor pace after pit
        FAILURE_TRAFFIC             // emerged into dirty air / traffic nullified position gain
    }

    private String driver;
    private int pitLapNumber;
    private String rivalAhead;              // car ahead at pit entry (undercut target), null if P1
    private String rivalBehind;             // car behind at pit entry (defense target), null if last
    private Double prePitGapAhead;          // directed gap to car ahead at pit entry (positive = behind)
    private Double prePitGapBehind;         // directed gap to car behind at pit entry
    private Double postPitGapToRival;       // gap to primary rival after resolution
    private String compound;                // tire compound after pit stop
    private Result result;
    private String trackStatusAtPit;        // track status code at pit entry, ex: "1" (green), "4" (sc)
    private int tyreAgeAtPit;               // laps completed on old tire set at pit entry
    private Double gapToCarAheadAtPit;      // raw gap in seconds to the car ahead when pitting
    private String race;                    // grand prix name, ex: "Italian Grand Prix"
    private Double gapDeltaPct;             // normalized gap change as % of baseline lap time
    private Double baselineLapTime;         // stint best used for normalization
    private boolean driverPittedFirst;      // true if this driver entered pit before primary rival
    private boolean isOffsetStrategy;       // true if resolved via timeout (1-stop vs 2-stop)
    private String resolvedVia;             // "RIVAL_PIT", "OFFSET_TIMEOUT", "SAFETY_TIMER"

    public PitStopEvaluationAlert() {
    }

    // builder-style construction via static factory to keep constructor manageable
    public static PitStopEvaluationAlert create(String driver, int pitLapNumber, Result result) {
        PitStopEvaluationAlert alert = new PitStopEvaluationAlert();
        alert.driver = driver;
        alert.pitLapNumber = pitLapNumber;
        alert.result = result;
        return alert;
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

    public String getRivalAhead() {
        return rivalAhead;
    }

    public void setRivalAhead(String rivalAhead) {
        this.rivalAhead = rivalAhead;
    }

    public String getRivalBehind() {
        return rivalBehind;
    }

    public void setRivalBehind(String rivalBehind) {
        this.rivalBehind = rivalBehind;
    }

    public Double getPrePitGapAhead() {
        return prePitGapAhead;
    }

    public void setPrePitGapAhead(Double prePitGapAhead) {
        this.prePitGapAhead = prePitGapAhead;
    }

    public Double getPrePitGapBehind() {
        return prePitGapBehind;
    }

    public void setPrePitGapBehind(Double prePitGapBehind) {
        this.prePitGapBehind = prePitGapBehind;
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

    public Double getGapDeltaPct() {
        return gapDeltaPct;
    }

    public void setGapDeltaPct(Double gapDeltaPct) {
        this.gapDeltaPct = gapDeltaPct;
    }

    public Double getBaselineLapTime() {
        return baselineLapTime;
    }

    public void setBaselineLapTime(Double baselineLapTime) {
        this.baselineLapTime = baselineLapTime;
    }

    public boolean isDriverPittedFirst() {
        return driverPittedFirst;
    }

    public void setDriverPittedFirst(boolean driverPittedFirst) {
        this.driverPittedFirst = driverPittedFirst;
    }

    public boolean isOffsetStrategy() {
        return isOffsetStrategy;
    }

    public void setOffsetStrategy(boolean offsetStrategy) {
        this.isOffsetStrategy = offsetStrategy;
    }

    public String getResolvedVia() {
        return resolvedVia;
    }

    public void setResolvedVia(String resolvedVia) {
        this.resolvedVia = resolvedVia;
    }

    // ml-ready csv row
    public String toCsvRow() {
        return String.join(",",
                safe(driver),
                String.valueOf(pitLapNumber),
                safe(rivalAhead),
                safe(rivalBehind),
                fmtDouble(prePitGapAhead),
                fmtDouble(prePitGapBehind),
                fmtDouble(postPitGapToRival),
                safe(compound),
                result != null ? result.name() : "",
                safe(trackStatusAtPit),
                String.valueOf(tyreAgeAtPit),
                fmtDouble(gapToCarAheadAtPit),
                safe(race),
                fmtDouble(gapDeltaPct),
                fmtDouble(baselineLapTime),
                String.valueOf(driverPittedFirst),
                String.valueOf(isOffsetStrategy),
                safe(resolvedVia)
        );
    }

    @Override
    public String toString() {
        return String.format(
                "PIT EVAL | %s Lap %d | %s | GapDelta: %s%% | Rivals: [%s, %s] | "
                + "PreGap: %s/%s | PostGap: %s | Offset: %s | Via: %s",
                driver, pitLapNumber, result,
                fmtDouble(gapDeltaPct),
                safe(rivalAhead), safe(rivalBehind),
                fmtDouble(prePitGapAhead), fmtDouble(prePitGapBehind),
                fmtDouble(postPitGapToRival),
                isOffsetStrategy, safe(resolvedVia));
    }

    private static String safe(String s) {
        return s != null ? s : "";
    }

    private static String fmtDouble(Double d) {
        return d != null ? String.format("%.3f", d) : "";
    }
}
