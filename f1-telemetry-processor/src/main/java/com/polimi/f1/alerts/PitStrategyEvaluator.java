package com.polimi.f1.alerts;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.events.LapEvent;
import com.polimi.f1.model.PitSuggestionAlert;

// computes a fuzzy-logic "pit desirability score" (0-100) for each driver on every lap,
// synthesizing four strategic dimensions into a single actionable metric.
//
// scoring dimensions:
//   pace:         +30 if lap time exceeds stint best by compound threshold
//   track status: +60 if SC or VSC active (cheap pit stop)
//   traffic:      -30 to +30. clean air (+30), easy pass (+5), DRS train (-30)
//   tire life:    -15 if next compound can't reach race end
//
// keyed by constant "RACE" for global position-ladder visibility, same design as
// DropZoneEvaluator. leader-driven trigger: P1 finishing lap N triggers evaluation
// of lap N-1, ensuring full-field data is available before scoring. internal MapState
// tracks per-driver strategy state and global max stint observations.
//
// emergent overcut behavior: when a rival pits, the gap ahead jumps to ~25s (clean air).
// pace improves without dirty air, so the pace score drops and traffic score becomes
// favorable. the evaluator naturally suppresses the pit suggestion, letting the driver
// "hammer time" and execute an overcut without explicit overcut logic.
public class PitStrategyEvaluator
        extends KeyedProcessFunction<String, LapEvent, PitSuggestionAlert> {

    // scoring thresholds
    private static final int EMIT_THRESHOLD = 60;
    private static final int PACE_SCORE = 30;
    private static final int TRACK_STATUS_SCORE = 60;
    private static final int CLEAN_AIR_SCORE = 30;
    private static final int EASY_PASS_SCORE = 5;
    private static final int DRS_TRAIN_PENALTY = -30;
    private static final int TIRE_LIFE_PENALTY = -15;

    // clean air gap threshold (seconds). emerging into > 3.0s gap means no dirty air.
    private static final double CLEAN_AIR_GAP = 3.0;

    // drs proximity threshold (seconds). cars within 1.0s are in a drs train.
    private static final double DRS_GAP_THRESHOLD = 1.0;

    // minimum tyre life on old tires of physical car ahead to qualify as easy pass.
    // a car on 25+ lap old tires is significantly slower, enables straightforward overtake.
    private static final int EASY_PASS_TYRE_LIFE = 25;

    // minimum tyre age (laps) before pit evaluation is meaningful (same as DropZoneEvaluator)
    private static final int MIN_TYRE_LIFE = 8;

    // percentage thresholds for pace score (matching TireDropDetector base values)
    private static final double SOFT_PCT = 0.012;
    private static final double MEDIUM_PCT = 0.015;
    private static final double HARD_PCT = 0.020;
    private static final double WET_PCT = 0.025;

    // default max stint estimates per compound when no observation is available yet.
    // conservative values: the system starts cautious and dynamically learns from the race.
    private static final int DEFAULT_SOFT_STINT = 18;
    private static final int DEFAULT_MEDIUM_STINT = 30;
    private static final int DEFAULT_HARD_STINT = 40;
    private static final int DEFAULT_WET_STINT = 25;

    // flat state accumulating all lap events, key format: "lapNumber:driver"
    // same pattern as DropZoneEvaluator to avoid flink's nested collection serialization pitfall
    private transient MapState<String, LapEvent> lapEvents;

    // per-driver strategy tracking, key = driver abbreviation (e.g., "VER")
    private transient MapState<String, DriverPitState> driverStates;

    // global maximum observed stint length per compound across all drivers.
    // key = compound name (e.g., "SOFT"), value = max tyre life observed at stint end.
    // dynamically updated when any driver pits, enabling data-driven stint estimates.
    private transient MapState<String, Integer> maxStintByCompound;

    @Override
    public void open(Configuration parameters) {
        lapEvents = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("lap-events", String.class, LapEvent.class));
        driverStates = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("driver-states", String.class, DriverPitState.class));
        maxStintByCompound = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("max-stint-compound",
                        Types.STRING, Types.INT));
    }

    private static String stateKey(int lap, String driver) {
        return lap + ":" + driver;
    }

    @Override
    public void processElement(LapEvent event, Context ctx, Collector<PitSuggestionAlert> out)
            throws Exception {
        int lap = event.getLapNumber();
        String driver = event.getDriver();

        // accumulate into flat state
        lapEvents.put(stateKey(lap, driver), event);

        // update global max stint tracking when a driver pits
        updateMaxStint(event);

        // update per-driver strategy state (stint best, consecutive slow laps)
        updateDriverState(event);

        // leader-driven trigger: when P1 finishes lap N, evaluate lap N-1
        if (event.getPosition() == 1 && lap > 1) {
            int previousLap = lap - 1;
            List<LapEvent> previousLapEvents = collectLap(previousLap);
            if (!previousLapEvents.isEmpty()) {
                evaluateAll(previousLapEvents, out);
                removeLap(previousLap, previousLapEvents);
            }
        }
    }

    // when a driver's stint changes (pit stop detected), record the tyre life they achieved
    // as the max observed for that compound. other drivers and future evaluations use this
    // to estimate how long a compound can realistically last at this track in these conditions.
    private void updateMaxStint(LapEvent event) throws Exception {
        DriverPitState state = driverStates.get(event.getDriver());
        if (state == null) {
            return;
        }

        // stint changed -> the previous stint just ended, record its tyre life
        if (state.getCurrentStint() != -1
                && state.getCurrentStint() != event.getStint()
                && state.getLastCompound() != null) {
            String prevCompound = state.getLastCompound();
            Integer current = maxStintByCompound.get(prevCompound);
            int previousTyreLife = state.getLastTyreLife();
            if (current == null || previousTyreLife > current) {
                maxStintByCompound.put(prevCompound, previousTyreLife);
            }
        }
    }

    // updates per-driver state: tracks stint transitions, stint best lap, and pace degradation
    private void updateDriverState(LapEvent event) throws Exception {
        String driver = event.getDriver();
        DriverPitState state = driverStates.get(driver);
        if (state == null) {
            state = new DriverPitState();
        }

        // stint change detection: reset pace tracking for new tire set
        if (state.getCurrentStint() != event.getStint()) {
            state.setCurrentStint(event.getStint());
            state.setStintBestLap(Double.MAX_VALUE);
            state.setConsecutiveSlowLaps(0);
        }

        // track the last known compound and tyre life for max stint calculation
        state.setLastCompound(event.getCompound());
        state.setLastTyreLife(event.getTyreLife());

        // update stint best with clean green-flag laps only
        Double lapTime = event.getLapTime();
        if (lapTime != null && lapTime > 0
                && event.getPitInTime() == null && event.getPitOutTime() == null
                && ("1".equals(event.getTrackStatus()) || event.getTrackStatus() == null)) {
            if (lapTime < state.getStintBestLap()) {
                state.setStintBestLap(lapTime);
            }

            // track consecutive slow laps for pace score
            double threshold = getBaseThreshold(event.getCompound());
            if (lapTime > state.getStintBestLap() * (1.0 + threshold)) {
                state.setConsecutiveSlowLaps(state.getConsecutiveSlowLaps() + 1);
            } else {
                state.setConsecutiveSlowLaps(0);
            }
        }

        driverStates.put(driver, state);
    }

    // collects all events for a given lap number from the flat state map
    private List<LapEvent> collectLap(int lap) throws Exception {
        String prefix = lap + ":";
        List<LapEvent> result = new ArrayList<>();
        for (Map.Entry<String, LapEvent> entry : lapEvents.entries()) {
            if (entry.getKey().startsWith(prefix)) {
                result.add(entry.getValue());
            }
        }
        return result;
    }

    // removes all entries for a completed lap to bound state memory growth
    private void removeLap(int lap, List<LapEvent> events) throws Exception {
        for (LapEvent e : events) {
            lapEvents.remove(stateKey(lap, e.getDriver()));
        }
    }

    // evaluates the pit desirability score for each eligible driver on the lap
    private void evaluateAll(List<LapEvent> laps, Collector<PitSuggestionAlert> out)
            throws Exception {
        laps.sort(Comparator.comparingInt(LapEvent::getPosition));

        for (int i = 0; i < laps.size(); i++) {
            LapEvent current = laps.get(i);

            // skip fresh tires, opening laps, pit laps
            if (current.getTyreLife() < MIN_TYRE_LIFE) {
                continue;
            }
            if (current.getPitInTime() != null || current.getPitOutTime() != null) {
                continue;
            }

            int paceScore = computePaceScore(current);
            int trackStatusScore = computeTrackStatusScore(current);

            // traffic analysis: walk position ladder to find emergence position
            TrafficResult traffic = computeTrafficResult(current, laps, i);
            int trafficScore = traffic.score;

            int tireLifePenalty = computeTireLifePenalty(current);

            int totalScore = paceScore + trackStatusScore + trafficScore + tireLifePenalty;
            totalScore = Math.max(0, Math.min(100, totalScore));

            if (totalScore >= EMIT_THRESHOLD) {
                String suggestion = buildSuggestion(paceScore, trackStatusScore,
                        trafficScore, tireLifePenalty);

                out.collect(new PitSuggestionAlert(
                        current.getDriver(),
                        current.getLapNumber(),
                        current.getPosition(),
                        current.getCompound(),
                        current.getTyreLife(),
                        totalScore,
                        paceScore,
                        trackStatusScore,
                        trafficScore,
                        tireLifePenalty,
                        current.getTrackStatus() != null ? current.getTrackStatus() : "1",
                        traffic.emergencePosition,
                        traffic.gapToPhysicalCar,
                        suggestion
                ));
            }
        }
    }

    // +30 if current pace has degraded beyond the compound-specific threshold.
    // uses per-driver state to compare against stint best, requiring at least
    // 1 consecutive slow lap to avoid one-off traffic spikes.
    private int computePaceScore(LapEvent current) throws Exception {
        DriverPitState state = driverStates.get(current.getDriver());
        if (state == null || current.getLapTime() == null) {
            return 0;
        }
        double best = state.getStintBestLap();
        if (best == Double.MAX_VALUE) {
            return 0;
        }
        double threshold = getBaseThreshold(current.getCompound());
        boolean paceDropped = current.getLapTime() > best * (1.0 + threshold);
        boolean sustained = state.getConsecutiveSlowLaps() >= 1;
        return (paceDropped && sustained) ? PACE_SCORE : 0;
    }

    // +60 if safety car or vsc is active. these create a "cheap pit stop" window
    // where the field is speed-limited, reducing relative pit loss from ~23s to ~10-15s.
    private static int computeTrackStatusScore(LapEvent current) {
        String status = current.getTrackStatus();
        if (status == null) {
            return 0;
        }
        return switch (status) {
            case "4", "6", "7" -> TRACK_STATUS_SCORE;
            default -> 0;
        };
    }

    // walks the position ladder to compute emergence position and scores traffic quality.
    // +30 clean air (>3.0s gap), +5 easy pass (old tires), -30 DRS train (multiple cars <1.0s)
    private TrafficResult computeTrafficResult(LapEvent current, List<LapEvent> laps,
            int posIndex) {
        TrafficResult result = new TrafficResult();
        result.emergencePosition = current.getPosition();

        Double pitLoss = selectPitLoss(current);
        if (pitLoss == null) {
            return result;
        }

        // walk the position ladder downward, accumulating gaps
        double cumulativeGap = 0;
        LapEvent physicalCarAhead = null;
        double gapToPhysicalCar = 0;

        for (int j = posIndex + 1; j < laps.size(); j++) {
            LapEvent behind = laps.get(j);
            Double gap = behind.getGapToCarAhead();
            if (gap == null) {
                break;
            }

            cumulativeGap += gap;

            if (cumulativeGap >= pitLoss) {
                // driver emerges between j-1 and j
                if (j == posIndex + 1) {
                    // gap behind > pitLoss, no positions lost, safe pit with clean air
                    result.score = CLEAN_AIR_SCORE;
                    return result;
                }
                physicalCarAhead = laps.get(j - 1);
                gapToPhysicalCar = pitLoss - (cumulativeGap - gap);
                break;
            }

            physicalCarAhead = behind;
            gapToPhysicalCar = pitLoss - cumulativeGap;
        }

        if (physicalCarAhead == null) {
            // no one behind or very small field, free pit
            result.score = CLEAN_AIR_SCORE;
            return result;
        }

        result.emergencePosition = physicalCarAhead.getPosition() + 1;
        result.gapToPhysicalCar = gapToPhysicalCar;

        // score based on emergence quality
        if (gapToPhysicalCar > CLEAN_AIR_GAP) {
            result.score = CLEAN_AIR_SCORE;
        } else if (isDrsTrainAtEmergence(laps, result.emergencePosition)) {
            result.score = DRS_TRAIN_PENALTY;
        } else if (physicalCarAhead.getTyreLife() >= EASY_PASS_TYRE_LIFE) {
            result.score = EASY_PASS_SCORE;
        } else {
            result.score = 0;
        }

        return result;
    }

    // checks if the emergence position lands in a DRS train: 2+ consecutive cars
    // within 1.0s gap near the emergence position, making overtaking ineffective
    // because each car in the train also has DRS from the car ahead.
    private static boolean isDrsTrainAtEmergence(List<LapEvent> laps, int emergencePos) {
        int trainCount = 0;
        for (int k = 0; k < laps.size(); k++) {
            LapEvent car = laps.get(k);
            // check cars around the emergence position (2 positions above and below)
            if (Math.abs(car.getPosition() - emergencePos) <= 2) {
                Double gap = car.getGapToCarAhead();
                if (gap != null && gap < DRS_GAP_THRESHOLD && gap > 0) {
                    trainCount++;
                }
            }
        }
        // >= 2 close gaps near emergence means a DRS train
        return trainCount >= 2;
    }

    // -15 if pitting now requires the next compound to exceed the max observed stint
    // for that compound. uses globally tracked max stint data learned from the race itself.
    // ex: 20 laps remaining, max observed MEDIUM stint = 28 -> ok (0 penalty)
    // ex: 20 laps remaining, max observed SOFT stint = 15 -> can't finish (-15)
    private int computeTireLifePenalty(LapEvent current) throws Exception {
        int totalLaps = current.getTotalLaps();
        if (totalLaps <= 0) {
            return 0;
        }

        int lapsRemaining = totalLaps - current.getLapNumber();
        if (lapsRemaining <= 0) {
            return 0;
        }

        String nextCompound = inferNextCompound(current.getCompound());
        Integer maxStint = maxStintByCompound.get(nextCompound);
        if (maxStint == null) {
            maxStint = defaultMaxStint(nextCompound);
        }

        return lapsRemaining > maxStint ? TIRE_LIFE_PENALTY : 0;
    }

    // builds a human-readable explanation string from the active scoring components.
    // ex: "Pace drop + SC opportunity + clean air"
    private static String buildSuggestion(int paceScore, int trackStatusScore,
            int trafficScore, int tireLifePenalty) {
        List<String> parts = new ArrayList<>();
        if (paceScore > 0) {
            parts.add("pace drop");
        }
        if (trackStatusScore > 0) {
            parts.add("SC/VSC opportunity");
        }
        if (trafficScore >= CLEAN_AIR_SCORE) {
            parts.add("clean air");
        } else if (trafficScore > 0) {
            parts.add("easy pass");
        } else if (trafficScore < 0) {
            parts.add("DRS train risk");
        }
        if (tireLifePenalty < 0) {
            parts.add("tight tire window");
        }
        return parts.isEmpty() ? "general" : String.join(" + ", parts);
    }

    // selects pit loss based on track status, same logic as DropZoneEvaluator.
    // green uses normal pit loss, sc/vsc use reduced pit loss values.
    // returns null for yellow/red flags to suppress evaluation.
    private static Double selectPitLoss(LapEvent lap) {
        String status = lap.getTrackStatus();
        if (status == null) {
            status = "1";
        }
        return switch (status) {
            case "1" -> lap.getPitLoss();
            case "6", "7" -> lap.getVscPitLoss();
            case "4" -> lap.getScPitLoss();
            default -> null;
        };
    }

    // base percentage threshold by compound, matching TireDropDetector values
    private static double getBaseThreshold(String compound) {
        if (compound == null) {
            return MEDIUM_PCT;
        }
        return switch (compound.toUpperCase()) {
            case "SOFT" -> SOFT_PCT;
            case "MEDIUM" -> MEDIUM_PCT;
            case "HARD" -> HARD_PCT;
            case "INTERMEDIATE", "WET" -> WET_PCT;
            default -> MEDIUM_PCT;
        };
    }

    // heuristic: assume the natural compound transition for strategy.
    // soft -> medium, medium -> hard, hard -> medium (reverse/alternative strategy)
    private static String inferNextCompound(String current) {
        if (current == null) {
            return "MEDIUM";
        }
        return switch (current.toUpperCase()) {
            case "SOFT" -> "MEDIUM";
            case "MEDIUM" -> "HARD";
            case "HARD" -> "MEDIUM";
            default -> "MEDIUM";
        };
    }

    // default stint length estimates per compound before race observations are available.
    // conservative values based on typical F1 stint lengths across the 2023 season.
    private static int defaultMaxStint(String compound) {
        if (compound == null) {
            return DEFAULT_MEDIUM_STINT;
        }
        return switch (compound.toUpperCase()) {
            case "SOFT" -> DEFAULT_SOFT_STINT;
            case "MEDIUM" -> DEFAULT_MEDIUM_STINT;
            case "HARD" -> DEFAULT_HARD_STINT;
            case "INTERMEDIATE", "WET" -> DEFAULT_WET_STINT;
            default -> DEFAULT_MEDIUM_STINT;
        };
    }

    // intermediate result from traffic analysis, avoids returning multiple values
    private static class TrafficResult {
        int score = 0;
        int emergencePosition = 0;
        double gapToPhysicalCar = 0;
    }

    // per-driver strategy tracking state, stored in MapState.
    // must be a pojo with no-arg constructor for flink serialization.
    public static class DriverPitState implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private int currentStint;
        private double stintBestLap;
        private int consecutiveSlowLaps;
        private String lastCompound;    // compound of the current/previous stint
        private int lastTyreLife;       // tyre life at the last observed lap

        public DriverPitState() {
            this.currentStint = -1;
            this.stintBestLap = Double.MAX_VALUE;
            this.consecutiveSlowLaps = 0;
        }

        public int getCurrentStint() { return currentStint; }
        public void setCurrentStint(int currentStint) { this.currentStint = currentStint; }

        public double getStintBestLap() { return stintBestLap; }
        public void setStintBestLap(double stintBestLap) { this.stintBestLap = stintBestLap; }

        public int getConsecutiveSlowLaps() { return consecutiveSlowLaps; }
        public void setConsecutiveSlowLaps(int consecutiveSlowLaps) { this.consecutiveSlowLaps = consecutiveSlowLaps; }

        public String getLastCompound() { return lastCompound; }
        public void setLastCompound(String lastCompound) { this.lastCompound = lastCompound; }

        public int getLastTyreLife() { return lastTyreLife; }
        public void setLastTyreLife(int lastTyreLife) { this.lastTyreLife = lastTyreLife; }
    }
}
