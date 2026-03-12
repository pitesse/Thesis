package com.polimi.f1.model.output;

// emitted when the fuzzy-logic pit desirability score for a driver exceeds the emit threshold.
// combines five scoring dimensions: pace degradation, track status (sc/vsc opportunity),
// traffic analysis (clean air vs drs train), tire life feasibility, and urgency (closing window).
//
// the score ranges 0-100 (clamped). individual components can be negative (traffic, tire life).
// urgencyScore adds +15/+30 based on current tyre age vs max stint for the compound.
// ex csv: VER,25,2,MEDIUM,18,75,30,0,30,-15,10,1,5,4.200,"Pace drop + clean air + closing window"
public class PitSuggestionAlert {

    public static final String CSV_HEADER
            = "driver,lapNumber,position,compound,tyreLife,totalScore,"
            + "paceScore,trackStatusScore,trafficScore,tireLifePenalty,urgencyScore,"
            + "trackStatus,emergencePosition,gapToPhysicalCar,suggestion";

    private String driver;
    private int lapNumber;
    private int position;
    private String compound;
    private int tyreLife;
    private int totalScore;         // 0-100 fuzzy desirability
    private int paceScore;          // 0-30
    private int trackStatusScore;   // 0-60
    private int trafficScore;       // -30 to +30
    private int tireLifePenalty;    // -15 to 0
    private int urgencyScore;       // 0 to +30, tyre age vs current compound max stint
    private String trackStatus;
    private int emergencePosition;  // computed physical position after pit
    private double gapToPhysicalCar;
    private String suggestion;      // human-readable summary, ex: "SC opportunity + clean air"

    public PitSuggestionAlert() {
    }

    public PitSuggestionAlert(String driver, int lapNumber, int position, String compound,
            int tyreLife, int totalScore, int paceScore, int trackStatusScore,
            int trafficScore, int tireLifePenalty, int urgencyScore, String trackStatus,
            int emergencePosition, double gapToPhysicalCar, String suggestion) {
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.position = position;
        this.compound = compound;
        this.tyreLife = tyreLife;
        this.totalScore = totalScore;
        this.paceScore = paceScore;
        this.trackStatusScore = trackStatusScore;
        this.trafficScore = trafficScore;
        this.tireLifePenalty = tireLifePenalty;
        this.urgencyScore = urgencyScore;
        this.trackStatus = trackStatus;
        this.emergencePosition = emergencePosition;
        this.gapToPhysicalCar = gapToPhysicalCar;
        this.suggestion = suggestion;
    }

    public String getDriver() { return driver; }
    public void setDriver(String driver) { this.driver = driver; }

    public int getLapNumber() { return lapNumber; }
    public void setLapNumber(int lapNumber) { this.lapNumber = lapNumber; }

    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }

    public String getCompound() { return compound; }
    public void setCompound(String compound) { this.compound = compound; }

    public int getTyreLife() { return tyreLife; }
    public void setTyreLife(int tyreLife) { this.tyreLife = tyreLife; }

    public int getTotalScore() { return totalScore; }
    public void setTotalScore(int totalScore) { this.totalScore = totalScore; }

    public int getPaceScore() { return paceScore; }
    public void setPaceScore(int paceScore) { this.paceScore = paceScore; }

    public int getTrackStatusScore() { return trackStatusScore; }
    public void setTrackStatusScore(int trackStatusScore) { this.trackStatusScore = trackStatusScore; }

    public int getTrafficScore() { return trafficScore; }
    public void setTrafficScore(int trafficScore) { this.trafficScore = trafficScore; }

    public int getTireLifePenalty() { return tireLifePenalty; }
    public void setTireLifePenalty(int tireLifePenalty) { this.tireLifePenalty = tireLifePenalty; }

    public int getUrgencyScore() { return urgencyScore; }
    public void setUrgencyScore(int urgencyScore) { this.urgencyScore = urgencyScore; }

    public String getTrackStatus() { return trackStatus; }
    public void setTrackStatus(String trackStatus) { this.trackStatus = trackStatus; }

    public int getEmergencePosition() { return emergencePosition; }
    public void setEmergencePosition(int emergencePosition) { this.emergencePosition = emergencePosition; }

    public double getGapToPhysicalCar() { return gapToPhysicalCar; }
    public void setGapToPhysicalCar(double gapToPhysicalCar) { this.gapToPhysicalCar = gapToPhysicalCar; }

    public String getSuggestion() { return suggestion; }
    public void setSuggestion(String suggestion) { this.suggestion = suggestion; }

    // ex: VER,25,2,MEDIUM,18,75,30,0,30,-15,10,1,5,4.200,Pace drop + clean air
    public String toCsvRow() {
        return String.join(",",
                driver != null ? driver : "",
                String.valueOf(lapNumber),
                String.valueOf(position),
                compound != null ? compound : "",
                String.valueOf(tyreLife),
                String.valueOf(totalScore),
                String.valueOf(paceScore),
                String.valueOf(trackStatusScore),
                String.valueOf(trafficScore),
                String.valueOf(tireLifePenalty),
                String.valueOf(urgencyScore),
                trackStatus != null ? trackStatus : "",
                String.valueOf(emergencePosition),
                String.format("%.3f", gapToPhysicalCar),
                suggestion != null ? suggestion : ""
        );
    }

    @Override
    public String toString() {
        return String.format(
                "PIT SUGGESTION | %s Lap %d P%d | %s L%d | Score: %d/100 "
                        + "[Pace:%d Status:%d Traffic:%d Penalty:%d Urgency:%d] | Emerge P%d (gap=%.1fs) | %s",
                driver, lapNumber, position, compound, tyreLife, totalScore,
                paceScore, trackStatusScore, trafficScore, tireLifePenalty, urgencyScore,
                emergencePosition, gapToPhysicalCar,
                suggestion != null ? suggestion : "");
    }
}
