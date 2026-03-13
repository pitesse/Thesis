package com.polimi.f1.state.groundtruth;

/**
 * snapshot of a rival driver at pit detection time. captures rival position
 * data including which lap the information was found, since position updates
 * may lag behind pit detection due to sequential finish line crossings.
 *
 * used during pit cycle registration to identify cars ahead and behind for gap
 * calculations.
 */
public class RivalSnapshot {

    /**
     * rival driver abbreviation (ex: "LEC", "VER")
     */
    public final String driver;

    /**
     * rival's stint number at pit detection time
     */
    public final int stint;

    /**
     * lap where this rival data was found (current lap or lap - 1)
     */
    public final int foundOnLap;

    /**
     * creates a rival snapshot with position data from a specific lap.
     *
     * @param driver rival driver abbreviation
     * @param stint rival's stint number when detected
     * @param foundOnLap lap where this position data was captured
     */
    public RivalSnapshot(String driver, int stint, int foundOnLap) {
        this.driver = driver;
        this.stint = stint;
        this.foundOnLap = foundOnLap;
    }

    /**
     * gets the rival driver abbreviation.
     *
     * @return driver abbreviation (ex: "LEC", "VER")
     */
    public String getDriver() {
        return driver;
    }

    /**
     * gets the rival's stint number at detection.
     *
     * @return stint number (1-based)
     */
    public int getStint() {
        return stint;
    }

    /**
     * gets the lap where this rival data was found.
     *
     * @return lap number where position was captured
     */
    public int getFoundOnLap() {
        return foundOnLap;
    }

    @Override
    public String toString() {
        return String.format("RivalSnapshot{driver='%s', stint=%d, foundOnLap=%d}",
                driver, stint, foundOnLap);
    }
}
