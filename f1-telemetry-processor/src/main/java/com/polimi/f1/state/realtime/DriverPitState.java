package com.polimi.f1.state.realtime;

import java.io.Serializable;

/**
 * per-driver strategy tracking state for pit desirability scoring. maintains
 * stint performance metrics including best lap times, pace ratios, and pace
 * degradation patterns used for continuous scoring calculation.
 *
 * follows flink pojo serialization requirements with private fields and public
 * accessors. serialization compatibility maintained via serialVersionUID for
 * state recovery.
 */
public class DriverPitState implements Serializable {

    private static final long serialVersionUID = 3L;

    // stint tracking
    private int currentStint = -1;
    private double stintBestLap = Double.MAX_VALUE;
    private int consecutiveSlowLaps = 0;

    // tire and compound tracking
    private String lastCompound;
    private int lastTyreLife;

    // pace performance tracking
    private double lastPaceRatio = 0.0;  // (lapTime - stintBest) / stintBest
    private double paceRatioDelta = 0.0; // current pace ratio minus previous valid ratio

    // local rival-pressure tracking
    private Double lastGapToCarAhead;
    private double gapToCarAheadDelta = 0.0; // current gap minus previous gap (positive = losing front car)

    /**
     * default no-argument constructor required for flink pojo serialization.
     */
    public DriverPitState() {
    }

    /**
     * gets the current stint number.
     *
     * @return stint number (1-based), -1 if not initialized
     */
    public int getCurrentStint() {
        return currentStint;
    }

    /**
     * sets the current stint number.
     *
     * @param currentStint stint number (1-based)
     */
    public void setCurrentStint(int currentStint) {
        this.currentStint = currentStint;
    }

    /**
     * gets the best lap time recorded in the current stint.
     *
     * @return best lap time in seconds, Double.MAX_VALUE if none recorded
     */
    public double getStintBestLap() {
        return stintBestLap;
    }

    /**
     * sets the best lap time in the current stint.
     *
     * @param stintBestLap best lap time in seconds
     */
    public void setStintBestLap(double stintBestLap) {
        this.stintBestLap = stintBestLap;
    }

    /**
     * gets the number of consecutive slow laps.
     *
     * @return count of consecutive laps above pace threshold
     */
    public int getConsecutiveSlowLaps() {
        return consecutiveSlowLaps;
    }

    /**
     * sets the number of consecutive slow laps.
     *
     * @param consecutiveSlowLaps count of consecutive slow laps
     */
    public void setConsecutiveSlowLaps(int consecutiveSlowLaps) {
        this.consecutiveSlowLaps = consecutiveSlowLaps;
    }

    /**
     * gets the last tire compound used.
     *
     * @return tire compound string (e.g., "SOFT", "MEDIUM", "HARD")
     */
    public String getLastCompound() {
        return lastCompound;
    }

    /**
     * sets the last tire compound used.
     *
     * @param lastCompound tire compound string
     */
    public void setLastCompound(String lastCompound) {
        this.lastCompound = lastCompound;
    }

    /**
     * gets the last tire life value.
     *
     * @return tire age in laps
     */
    public int getLastTyreLife() {
        return lastTyreLife;
    }

    /**
     * sets the last tire life value.
     *
     * @param lastTyreLife tire age in laps
     */
    public void setLastTyreLife(int lastTyreLife) {
        this.lastTyreLife = lastTyreLife;
    }

    /**
     * gets the last pace ratio relative to stint best.
     *
     * @return pace ratio (lapTime - stintBest) / stintBest
     */
    public double getLastPaceRatio() {
        return lastPaceRatio;
    }

    /**
     * sets the last pace ratio relative to stint best.
     *
     * @param lastPaceRatio pace ratio calculation
     */
    public void setLastPaceRatio(double lastPaceRatio) {
        this.lastPaceRatio = lastPaceRatio;
    }

    /**
     * gets the pace ratio variation between the two latest valid laps.
     *
     * @return current pace ratio minus previous pace ratio
     */
    public double getPaceRatioDelta() {
        return paceRatioDelta;
    }

    /**
     * sets the pace ratio variation between the two latest valid laps.
     *
     * @param paceRatioDelta pace ratio delta
     */
    public void setPaceRatioDelta(double paceRatioDelta) {
        this.paceRatioDelta = paceRatioDelta;
    }

    /**
     * gets the last observed gap to the car ahead.
     *
     * @return gap in seconds, null if unavailable
     */
    public Double getLastGapToCarAhead() {
        return lastGapToCarAhead;
    }

    /**
     * sets the last observed gap to the car ahead.
     *
     * @param lastGapToCarAhead gap in seconds
     */
    public void setLastGapToCarAhead(Double lastGapToCarAhead) {
        this.lastGapToCarAhead = lastGapToCarAhead;
    }

    /**
     * gets the variation of gap to the car ahead between latest observations.
     *
     * @return current gap minus previous gap
     */
    public double getGapToCarAheadDelta() {
        return gapToCarAheadDelta;
    }

    /**
     * sets the variation of gap to the car ahead between latest observations.
     *
     * @param gapToCarAheadDelta current gap minus previous gap
     */
    public void setGapToCarAheadDelta(double gapToCarAheadDelta) {
        this.gapToCarAheadDelta = gapToCarAheadDelta;
    }

    @Override
    public String toString() {
        return String.format(
                "DriverPitState{stint=%d, bestLap=%.3f, slowLaps=%d, compound='%s', tyreLife=%d, paceRatio=%.4f, "
                + "paceDelta=%.4f, gapAhead=%s, gapDelta=%.4f}",
                currentStint,
                stintBestLap,
                consecutiveSlowLaps,
                lastCompound,
                lastTyreLife,
                lastPaceRatio,
                paceRatioDelta,
                String.valueOf(lastGapToCarAhead),
                gapToCarAheadDelta);
    }
}
