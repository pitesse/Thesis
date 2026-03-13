package com.polimi.f1.operators.realtime;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.output.TireDropAlert;

// detects tire performance degradation using a percentage-based "thermal cliff" approach.
// thresholds are expressed as a percentage of stint-best lap time, so a 1s drop on a 75s
// circuit (monaco) is weighted differently than on a 90s circuit (spa).
//
// dynamic threshold adjustments:
//   fuel burn: cars start ~110kg, burn ~1.8kg/lap. lighter = faster, so we linearly
//     reduce the threshold across the race making detection more sensitive. total: 0.3% at race end.
//   track temperature: surface temps >40C accelerate graining/blistering, subtract 0.2% from threshold.
//
// requires 2 consecutive slow laps before emitting an alert. single-lap spikes (traffic,
// mistake, brief yellow) are filtered, two consecutive laps confirm a systematic tire cliff.
public class TireDropDetector extends KeyedProcessFunction<String, LapEvent, TireDropAlert> {

    // percentage-based thresholds: lapTime > stintBest * (1 + threshold) signals degradation.
    // softer compounds degrade faster, so their baseline threshold is tighter.
    // ex: stintBest=80.0s, SOFT threshold=1.2% -> alert if lapTime > 80.96s for 2 consecutive laps
    private static final double SOFT_BASE_PCT = 0.012;
    private static final double MEDIUM_BASE_PCT = 0.015;
    private static final double HARD_BASE_PCT = 0.020;
    private static final double WET_BASE_PCT = 0.025;

    // fuel burn adjustment: cars lose ~1.8kg/lap from ~110kg starting fuel.
    // lighter car = inherently faster -> same lap time degradation indicates worse tires.
    // linearly reduces threshold across the race, making detection more sensitive.
    // total adjustment at race end = 0.3% (~0.24s on an 80s baseline lap).
    // ex: lap 30/60, SOFT -> 0.012 - (0.003 * 30/60) = 0.0105
    private static final double FUEL_BURN_TOTAL_PCT = 0.003;

    // high track temperature accelerates rubber degradation (graining, blistering).
    // subtract from threshold when trackTemp > 40C to increase sensitivity.
    private static final double HOT_TRACK_THRESHOLD = 40.0;
    private static final double HOT_TRACK_ADJUSTMENT = 0.002;

    // floor to prevent threshold from going negative on late-race hot laps
    private static final double MIN_THRESHOLD_PCT = 0.005;

    // consecutive laps above threshold required before emitting alert.
    // single-lap spikes (traffic, mistake) are filtered, two consecutive laps
    // confirm a systematic tire performance cliff.
    private static final int CONSECUTIVE_REQUIRED = 2;

    // current stint number, used to detect stint changes (pit stop -> new tires)
    private transient ValueState<Integer> currentStint;
    // best clean lap time in the current stint (seconds)
    private transient ValueState<Double> stintBestLap;
    // consecutive laps exceeding the degradation threshold
    private transient ValueState<Integer> consecutiveSlowLaps;

    @Override
    public void open(OpenContext openContext) {
        currentStint = getRuntimeContext().getState(
                new ValueStateDescriptor<>("current-stint", Types.INT));
        stintBestLap = getRuntimeContext().getState(
                new ValueStateDescriptor<>("stint-best-lap", Types.DOUBLE));
        consecutiveSlowLaps = getRuntimeContext().getState(
                new ValueStateDescriptor<>("consecutive-slow-laps", Types.INT));
    }

    @Override
    public void processElement(LapEvent lap, Context ctx, Collector<TireDropAlert> out) throws Exception {
        // detect stint change: reset all state for new tire set
        Integer prevStint = currentStint.value();
        if (prevStint == null || prevStint != lap.getStint()) {
            currentStint.update(lap.getStint());
            stintBestLap.update(Double.MAX_VALUE);
            consecutiveSlowLaps.update(0);
        }

        // skip opening laps, standing start and traffic make pace non-representative
        if (lap.getLapNumber() <= 2) {
            return;
        }

        // skip pit in-laps and out-laps, they distort the baseline
        if (lap.getPitInTime() != null || lap.getPitOutTime() != null) {
            return;
        }

        // keep only green-flag racing laps, ex trackStatus=1
        if (lap.getTrackStatus() != null && !lap.getTrackStatus().equals("1")) {
            return;
        }

        // skip laps with missing lap time (can happen with data quality issues)
        Double lapTimeSec = lap.getLapTime();
        if (lapTimeSec == null || lapTimeSec <= 0) {
            return;
        }

        // update stint best
        double best = stintBestLap.value();
        if (lapTimeSec < best) {
            stintBestLap.update(lapTimeSec);
            best = lapTimeSec;
        }

        // compute dynamic threshold: base percentage adjusted for fuel burn and track temp
        double baseThreshold = getBaseThresholdForCompound(lap.getCompound());
        double fuelAdjust = computeFuelAdjustment(lap.getLapNumber(), lap.getTotalLaps());
        double hotAdjust = computeHotTrackAdjustment(lap.getTrackTemp());
        double threshold = Math.max(baseThreshold - fuelAdjust - hotAdjust, MIN_THRESHOLD_PCT);

        double thresholdTime = best * (1.0 + threshold);

        if (lapTimeSec > thresholdTime) {
            int consecutive = consecutiveSlowLaps.value() != null ? consecutiveSlowLaps.value() : 0;
            consecutive++;
            consecutiveSlowLaps.update(consecutive);

            if (consecutive >= CONSECUTIVE_REQUIRED) {
                double delta = lapTimeSec - best;
                out.collect(new TireDropAlert(
                        lap.getDriver(),
                        lap.getLapNumber(),
                        lap.getCompound(),
                        lap.getTyreLife(),
                        lapTimeSec,
                        best,
                        delta
                ));
                // reset after emission so we don't fire every subsequent lap
                consecutiveSlowLaps.update(0);
            }
        } else {
            // good lap breaks the streak
            consecutiveSlowLaps.update(0);
        }
    }

    // base percentage threshold by compound, ex: "SOFT" -> 0.012 (1.2%)
    private static double getBaseThresholdForCompound(String compound) {
        if (compound == null) {
            return MEDIUM_BASE_PCT;
        }
        return switch (compound.toUpperCase()) {
            case "SOFT" ->
                SOFT_BASE_PCT;
            case "MEDIUM" ->
                MEDIUM_BASE_PCT;
            case "HARD" ->
                HARD_BASE_PCT;
            case "INTERMEDIATE", "WET" ->
                WET_BASE_PCT;
            default ->
                MEDIUM_BASE_PCT;
        };
    }

    // fuel burn makes car lighter -> faster -> tires appear to degrade less.
    // compensate by reducing threshold linearly across the race.
    // ex: lap 30/60 -> fuelAdjust = 0.003 * (30/60) = 0.0015
    private static double computeFuelAdjustment(int lapNumber, int totalLaps) {
        if (totalLaps <= 0) {
            return 0.0;
        }
        double progress = Math.min((double) lapNumber / totalLaps, 1.0);
        return FUEL_BURN_TOTAL_PCT * progress;
    }

    // extreme track surface temperature accelerates rubber grain/blister formation,
    // lowering the threshold to catch the earlier onset of degradation.
    // ex: trackTemp=45.0 -> subtract 0.002 from threshold
    private static double computeHotTrackAdjustment(Double trackTemp) {
        if (trackTemp != null && trackTemp > HOT_TRACK_THRESHOLD) {
            return HOT_TRACK_ADJUSTMENT;
        }
        return 0.0;
    }
}
