package com.polimi.f1.model.output;

import com.polimi.f1.model.TrackStatusCodes;

// emitted when the continuous fuzzy-logic pit desirability score crosses the emit threshold.
// combines six scoring dimensions: continuous pace degradation, track status (sc/vsc opportunity),
// continuous traffic analysis (gap-based interpolation), tire urgency (quadratic ramp),
// strategy penalty (compound feasibility), and end-of-race suppression (logistic sigmoid).
//
// all scores are doubles for continuous distributions (no more int clumping).
// suggestionLabel provides discrete classification: MONITOR, GOOD_PIT, PIT_NOW, LOST_CHANCE.
//
// ex csv: Italian Grand Prix,VER,25,2023-09-03T13:05:12.003,2,MEDIUM,18,75.3,22.1,0,18.7,-4.2,8.7,-2.5,1,5,4.200,GOOD_PIT,"pace drop + clean air"
public class PitSuggestionAlert {

    public static final String CSV_HEADER
            = "race,driver,lapNumber,eventDate,position,compound,tyreLife,totalScore,"
            + "paceScore,trackStatusScore,trafficScore,strategyPenalty,urgencyScore,"
            + "endOfRacePenalty,trackStatus,emergencePosition,gapToPhysicalCar,suggestionLabel,suggestion";

    // discrete labels derived from continuous totalScore
    public enum SuggestionLabel {
        MONITOR, // score 40-59, watch situation
        GOOD_PIT, // score 60-79, window open
        PIT_NOW, // score 80+, immediate action
        LOST_CHANCE          // special: peak score was >= 70 but dropped below 40
    }

    private String race;
    private String driver;
    private int lapNumber;
    private String eventDate;
    private int position;
    private String compound;
    private int tyreLife;
    private double totalScore;           // 0.0-100.0 continuous
    private double paceScore;            // 0.0-30.0 continuous (power 1.5 curve)
    private double trackStatusScore;     // 0.0 or 60.0 (crisp, binary event)
    private double trafficScore;         // -30.0 to +30.0 continuous (linear interpolation)
    private double strategyPenalty;      // -15.0 to 0.0 continuous (replaces tireLifePenalty)
    private double urgencyScore;         // 0.0-30.0 continuous (quadratic ramp)
    private double endOfRacePenalty;     // -100.0 to 0.0 continuous (logistic sigmoid)
    private String trackStatus;
    private int emergencePosition;
    private double gapToPhysicalCar;
    private String suggestionLabel;      // "MONITOR", "GOOD_PIT", "PIT_NOW", "LOST_CHANCE"
    private String suggestion;           // human-readable summary

    public PitSuggestionAlert() {
    }

    // args grouped as race context, score components, and tactical emergence data
    public PitSuggestionAlert(String race, String driver, int lapNumber, String eventDate,
            int position, String compound,
            int tyreLife, double totalScore, double paceScore, double trackStatusScore,
            double trafficScore, double strategyPenalty, double urgencyScore,
            double endOfRacePenalty, String trackStatus,
            int emergencePosition, double gapToPhysicalCar,
            String suggestionLabel, String suggestion) {
        this.race = race;
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.eventDate = eventDate;
        this.position = position;
        this.compound = compound;
        this.tyreLife = tyreLife;
        this.totalScore = totalScore;
        this.paceScore = paceScore;
        this.trackStatusScore = trackStatusScore;
        this.trafficScore = trafficScore;
        this.strategyPenalty = strategyPenalty;
        this.urgencyScore = urgencyScore;
        this.endOfRacePenalty = endOfRacePenalty;
        this.trackStatus = trackStatus;
        this.emergencePosition = emergencePosition;
        this.gapToPhysicalCar = gapToPhysicalCar;
        this.suggestionLabel = suggestionLabel;
        this.suggestion = suggestion;
    }

    public String getRace() {
        return race;
    }

    public void setRace(String race) {
        this.race = race;
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

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
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

    public double getTotalScore() {
        return totalScore;
    }

    public void setTotalScore(double totalScore) {
        this.totalScore = totalScore;
    }

    public double getPaceScore() {
        return paceScore;
    }

    public void setPaceScore(double paceScore) {
        this.paceScore = paceScore;
    }

    public double getTrackStatusScore() {
        return trackStatusScore;
    }

    public void setTrackStatusScore(double trackStatusScore) {
        this.trackStatusScore = trackStatusScore;
    }

    public double getTrafficScore() {
        return trafficScore;
    }

    public void setTrafficScore(double trafficScore) {
        this.trafficScore = trafficScore;
    }

    public double getStrategyPenalty() {
        return strategyPenalty;
    }

    public void setStrategyPenalty(double strategyPenalty) {
        this.strategyPenalty = strategyPenalty;
    }

    public double getUrgencyScore() {
        return urgencyScore;
    }

    public void setUrgencyScore(double urgencyScore) {
        this.urgencyScore = urgencyScore;
    }

    public double getEndOfRacePenalty() {
        return endOfRacePenalty;
    }

    public void setEndOfRacePenalty(double endOfRacePenalty) {
        this.endOfRacePenalty = endOfRacePenalty;
    }

    public String getTrackStatus() {
        return trackStatus;
    }

    public void setTrackStatus(String trackStatus) {
        this.trackStatus = trackStatus;
    }

    public int getEmergencePosition() {
        return emergencePosition;
    }

    public void setEmergencePosition(int emergencePosition) {
        this.emergencePosition = emergencePosition;
    }

    public double getGapToPhysicalCar() {
        return gapToPhysicalCar;
    }

    public void setGapToPhysicalCar(double gapToPhysicalCar) {
        this.gapToPhysicalCar = gapToPhysicalCar;
    }

    public String getSuggestionLabel() {
        return suggestionLabel;
    }

    public void setSuggestionLabel(String suggestionLabel) {
        this.suggestionLabel = suggestionLabel;
    }

    public String getSuggestion() {
        return suggestion;
    }

    public void setSuggestion(String suggestion) {
        this.suggestion = suggestion;
    }

    // ex: Italian Grand Prix,VER,25,2023-09-03T13:05:12.003,2,MEDIUM,18,75.300,22.1,0,18.7,-4.2,8.7,-2.5,1,5,4.200,GOOD_PIT,pace drop + clean air
    public String toCsvRow() {
        return String.join(",",
                race != null ? race : "",
                driver != null ? driver : "",
                String.valueOf(lapNumber),
                eventDate != null ? eventDate : "",
                String.valueOf(position),
                compound != null ? compound : "",
                String.valueOf(tyreLife),
                String.format("%.1f", totalScore),
                String.format("%.1f", paceScore),
                String.format("%.1f", trackStatusScore),
                String.format("%.1f", trafficScore),
                String.format("%.1f", strategyPenalty),
                String.format("%.1f", urgencyScore),
                String.format("%.1f", endOfRacePenalty),
                TrackStatusCodes.normalizeOrGreen(trackStatus),
                String.valueOf(emergencePosition),
                String.format("%.3f", gapToPhysicalCar),
                suggestionLabel != null ? suggestionLabel : "",
                suggestion != null ? suggestion : ""
        );
    }

    @Override
    public String toString() {
        return String.format(
                "PIT SUGGESTION | %s | %s Lap %d P%d | %s L%d | Score: %.1f/100 [%s] "
                + "[Pace:%.1f Status:%.1f Traffic:%.1f Penalty:%.1f Urgency:%.1f EoR:%.1f] "
                + "| Emerge P%d (gap=%.1fs) | %s",
                race != null ? race : "?",
                driver, lapNumber, position, compound, tyreLife, totalScore,
                suggestionLabel != null ? suggestionLabel : "?",
                paceScore, trackStatusScore, trafficScore, strategyPenalty,
                urgencyScore, endOfRacePenalty,
                emergencePosition, gapToPhysicalCar,
                suggestion != null ? suggestion : "");
    }
}
