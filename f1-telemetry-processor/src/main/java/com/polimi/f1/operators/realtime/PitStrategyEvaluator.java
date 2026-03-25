package com.polimi.f1.operators.realtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polimi.f1.model.TrackStatusCodes;
import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.input.TrackStatusEvent;
import com.polimi.f1.model.output.PitSuggestionAlert;
import com.polimi.f1.model.output.PitSuggestionAlert.SuggestionLabel;
import com.polimi.f1.state.realtime.DriverPitState;

// computes a continuous fuzzy-logic "pit desirability score" (0.0-100.0) for each driver,
// using fully continuous scoring curves that eliminate the discrete score clumping of the
// previous implementation.
//
// architecture: KeyedBroadcastProcessFunction receiving lap events (keyed by "RACE")
// and broadcast track status changes. strategy is evaluated instantly using a
// continuous O(1) snapshot of the physical grid from latestGridState,
// filtering out retired or ghost cars.
//
// scoring dimensions (all continuous except track status):
//   pace:     0-30, power 1.5 curve: 30 * min(1.0, (paceRatio / 0.03)^1.5)
//   traffic: -30 to +30, linear interpolation based on emergence gap
//   urgency:  0-30, quadratic ramp: 30 * min(1.0, ((tyreRatio - 0.5) / 0.5)^2)
//   strategy: -15 to 0, continuous deficit penalty
//   track:    0 or 60 (crisp, binary event, sc/vsc only)
//   eor:     -100 to 0, logistic sigmoid: -100 / (1 + e^(-15*(ratio-0.92)))
//            near-zero until 85%, ramps through 90-95%, effectively -100 at 98%+.
//            under SC at 90%: +60 track overcomes ~-18 eor penalty -> GOOD_PIT.
//            under SC at 95%+: even +60 can't overcome ~-89 eor -> correctly suppressed.
//
// multi-label output: MONITOR (40-59), GOOD_PIT (60-79), PIT_NOW (80+), LOST_CHANCE (peak decay)
//
// emit-gate: suppresses notification spam by tracking per-driver emissions within a stint.
// first alert above threshold fires immediately. subsequent laps only re-emit if the score
// has increased by >= 10 points (escalation) or track status changed (new opportunity).
//
// lost_chance detection: tracks per-driver peak score. if score was >= 70 but drops below 40
// while degradation worsens, emits LOST_CHANCE once per stint.
//
// keyed by constant "RACE" for global position-ladder visibility (same as DropZoneEvaluator).
// max-lap trigger advances global race progress and refreshes snapshot-based evaluation.
public class PitStrategyEvaluator
        extends KeyedBroadcastProcessFunction<String, LapEvent, TrackStatusEvent, PitSuggestionAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(PitStrategyEvaluator.class);

    // broadcast state descriptor, shared with F1StreamingJob for .broadcast() call.
    // same pattern as TrackStatusEnricher but separate instance for this operator.
    public static final MapStateDescriptor<String, String> TRACK_STATUS_STATE
            = new MapStateDescriptor<>(
                    "pit-strategy-track-status",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
            );

    // emit threshold: minimum score to generate an alert
    private static final double EMIT_THRESHOLD = 40.0;
    private static final double GOOD_PIT_THRESHOLD = 60.0;
    private static final double PIT_NOW_THRESHOLD = 80.0;

    // emit-gate: minimum score increase since last emission before re-emitting
    private static final double RE_EMIT_DELTA = 10.0;
    private static final double SLOW_LAP_RATIO_THRESHOLD = 0.005;
    private static final int RECENT_LAP_WINDOW = 3;
    private static final String CURRENT_STATUS_KEY = "current";
    private static final String LATEST_LAP_KEY = "latest";
    private static final double PACE_CURVE_POWER = 1.5;

    // track status score: +60 for sc/vsc (crisp, binary event)
    private static final int TRACK_STATUS_SCORE = 60;

    // pace curve: paceRatio at which score reaches 30 (fully degraded)
    // ex: at 3% degradation vs stint best, pace score = 30
    private static final double PACE_CEILING_RATIO = 0.03;

    // traffic thresholds (seconds)
    private static final double CLEAN_AIR_GAP = 3.0;
    private static final double DRS_THRESHOLD = 1.0;

    // urgency: quadratic ramp starts at 50% of max stint
    private static final double URGENCY_ONSET_RATIO = 0.5;

    // tyre life bonus for easy pass of car ahead with old tires
    private static final int EASY_PASS_TYRE_LIFE = 25;
    private static final double EASY_PASS_BONUS = 5.0;

    // minimum tyre age before evaluation is meaningful
    private static final int MIN_TYRE_LIFE = 8;

    // lost chance detection: peak score threshold and drop threshold
    private static final double LOST_CHANCE_PEAK = 70.0;
    private static final double LOST_CHANCE_DROP = 40.0;

    // default max stint estimates per compound when no observation is available yet
    private static final int DEFAULT_SOFT_STINT = 18; //TODO these values may be better as percenteges of race length rather than fixed lap counts
    private static final int DEFAULT_MEDIUM_STINT = 30;
    private static final int DEFAULT_HARD_STINT = 40;
    private static final int DEFAULT_WET_STINT = 25;

    // end-of-race sigmoid: steepness of the logistic curve.
    // k=15 produces a sharp transition centered at the midpoint (0.92),
    // near-zero below 85%, effectively -100 above 98%.
    private static final double EOR_SIGMOID_K = 15.0;

    // end-of-race sigmoid: midpoint of the logistic curve (92% race completion).
    // at this point, penalty = -50. chosen so that the "cliff" where pitting becomes
    // meaningless aligns with ~4 laps remaining in a 50-lap race.
    private static final double EOR_SIGMOID_MIDPOINT = 0.92;

    // per-driver strategy tracking, key = driver abbreviation
    private transient MapState<String, DriverPitState> driverStates;

    // latest event per driver, used to build an always-current full grid snapshot
    private transient MapState<String, LapEvent> latestGridState;

    // global maximum observed stint length per compound across all drivers
    private transient MapState<String, Integer> maxStintByCompound;

    // emit-gate: score at last emission per driver
    private transient MapState<String, Double> lastEmittedScore;

    // emit-gate: stint number at last emission per driver
    private transient MapState<String, Integer> lastEmittedStint;

    // emit-gate: track status at last emission per driver
    private transient MapState<String, String> lastEmittedTrackStatus;

    // per-driver peak score tracking for LOST_CHANCE detection
    private transient MapState<String, Double> peakScores;

    // per-driver flag: whether LOST_CHANCE has been emitted this stint
    private transient MapState<String, Boolean> lostChanceEmitted;

    // cache latest observed race progress for broadcast urgency fast-path
    private transient MapState<String, Integer> lastCompletedLap;

    // max observed lap across all events, used as a stall-safe progress trigger
    private transient ValueState<Integer> maxLapState;

    @Override
    public void open(OpenContext openContext) {
        // 2h ttl: prevents unbounded state growth over continuous streaming
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, DriverPitState> driverDesc
                = new MapStateDescriptor<>("strategy-driver-states", String.class, DriverPitState.class);
        driverDesc.enableTimeToLive(ttlConfig);
        driverStates = getRuntimeContext().getMapState(driverDesc);

        MapStateDescriptor<String, LapEvent> latestGridDesc
                = new MapStateDescriptor<>("strategy-latest-grid", Types.STRING, Types.POJO(LapEvent.class));
        latestGridDesc.enableTimeToLive(ttlConfig);
        latestGridState = getRuntimeContext().getMapState(latestGridDesc);

        MapStateDescriptor<String, Integer> maxStintDesc
                = new MapStateDescriptor<>("strategy-max-stint", Types.STRING, Types.INT);
        maxStintDesc.enableTimeToLive(ttlConfig);
        maxStintByCompound = getRuntimeContext().getMapState(maxStintDesc);

        MapStateDescriptor<String, Double> emitScoreDesc
                = new MapStateDescriptor<>("strategy-emit-score", String.class, Double.class);
        emitScoreDesc.enableTimeToLive(ttlConfig);
        lastEmittedScore = getRuntimeContext().getMapState(emitScoreDesc);

        MapStateDescriptor<String, Integer> emitStintDesc
                = new MapStateDescriptor<>("strategy-emit-stint", Types.STRING, Types.INT);
        emitStintDesc.enableTimeToLive(ttlConfig);
        lastEmittedStint = getRuntimeContext().getMapState(emitStintDesc);

        MapStateDescriptor<String, String> emitTsDesc
                = new MapStateDescriptor<>("strategy-emit-track-status", Types.STRING, Types.STRING);
        emitTsDesc.enableTimeToLive(ttlConfig);
        lastEmittedTrackStatus = getRuntimeContext().getMapState(emitTsDesc);

        MapStateDescriptor<String, Double> peakDesc
                = new MapStateDescriptor<>("strategy-peak-scores", String.class, Double.class);
        peakDesc.enableTimeToLive(ttlConfig);
        peakScores = getRuntimeContext().getMapState(peakDesc);

        MapStateDescriptor<String, Boolean> lostDesc
                = new MapStateDescriptor<>("strategy-lost-chance", String.class, Boolean.class);
        lostDesc.enableTimeToLive(ttlConfig);
        lostChanceEmitted = getRuntimeContext().getMapState(lostDesc);

        MapStateDescriptor<String, Integer> lastLapDesc
                = new MapStateDescriptor<>("strategy-last-completed-lap", Types.STRING, Types.INT);
        lastLapDesc.enableTimeToLive(ttlConfig);
        lastCompletedLap = getRuntimeContext().getMapState(lastLapDesc);

        ValueStateDescriptor<Integer> maxLapDesc
                = new ValueStateDescriptor<>("strategy-max-lap", Types.INT);
        maxLapDesc.enableTimeToLive(ttlConfig);
        maxLapState = getRuntimeContext().getState(maxLapDesc);
    }

    // data-driven evaluation path, updates snapshot and triggers scoring on race progress
    @Override
    public void processElement(LapEvent event,
            KeyedBroadcastProcessFunction<String, LapEvent, TrackStatusEvent, PitSuggestionAlert>.ReadOnlyContext ctx,
            Collector<PitSuggestionAlert> out) throws Exception {
        if (event == null || event.getDriver() == null || event.getLapNumber() <= 0) {
            return;
        }

        int lap = event.getLapNumber();
        String driver = event.getDriver();

        latestGridState.put(driver, event);
        updateMaxStint(event);
        updateDriverState(event);

        Integer maxLap = maxLapState.value();
        if (maxLap == null) {
            maxLap = 0;
        }

        if (lap > maxLap) {
            maxLapState.update(lap);
            if (lap > 1) {
                int previousLap = lap - 1;
                lastCompletedLap.put(LATEST_LAP_KEY, previousLap);
                List<LapEvent> currentGrid = collectFreshGrid(lap);
                if (!currentGrid.isEmpty()) {
                    String trackStatus = readTrackStatus(ctx);
                    evaluateAll(currentGrid, trackStatus, out);
                }
            }
        }
    }

    // broadcast-driven urgency path, fires immediately when SC/VSC deploys
    // re-evaluates the filtered physical-grid snapshot with current track status
    @Override
    public void processBroadcastElement(TrackStatusEvent statusEvent,
            KeyedBroadcastProcessFunction<String, LapEvent, TrackStatusEvent, PitSuggestionAlert>.Context ctx,
            Collector<PitSuggestionAlert> out) throws Exception {
        if (statusEvent == null || statusEvent.getStatus() == null) {
            return;
        }

        ctx.getBroadcastState(TRACK_STATUS_STATE).put(CURRENT_STATUS_KEY, statusEvent.getStatus());

        String status = statusEvent.getStatus();
        if (!TrackStatusCodes.isCaution(status)) {
            return; // only trigger urgency on SC/VSC/VSCEnding
        }

        LOG.info("sc/vsc urgency trigger: status={}", status);

        // re-evaluate all drivers using latest observed race progress
        Integer latestLap = lastCompletedLap.get(LATEST_LAP_KEY);
        if (latestLap == null) {
            return;
        }

        List<LapEvent> currentGrid = collectFreshGrid(latestLap);
        if (!currentGrid.isEmpty()) {
            evaluateAll(currentGrid, status, out);
        }
    }

    // reads current track status from broadcast state, defaulting to green
    private String readTrackStatus(
            KeyedBroadcastProcessFunction<String, LapEvent, TrackStatusEvent, PitSuggestionAlert>.ReadOnlyContext ctx)
            throws Exception {
        String status = ctx.getBroadcastState(TRACK_STATUS_STATE).get(CURRENT_STATUS_KEY);
        return TrackStatusCodes.normalizeOrGreen(status);
    }

    // updates max observed tyre life per compound in real time, each lap
    private void updateMaxStint(LapEvent event) throws Exception {
        String compound = event.getCompound();
        if (compound == null) {
            return;
        }

        Integer currentMax = maxStintByCompound.get(compound);
        int tyreLife = event.getTyreLife();
        if (currentMax == null || tyreLife > currentMax) {
            maxStintByCompound.put(compound, tyreLife);
        }
    }

    // updates per-driver state: stint transitions, stint best lap, pace tracking
    private void updateDriverState(LapEvent event) throws Exception {
        String driver = event.getDriver();
        DriverPitState state = driverStates.get(driver);
        if (state == null) {
            state = new DriverPitState();
        }

        // stint change: reset pace tracking, peak score, lost chance flag
        if (state.getCurrentStint() != event.getStint()) {
            state.setCurrentStint(event.getStint());
            state.setStintBestLap(Double.MAX_VALUE);
            state.setConsecutiveSlowLaps(0);
            state.setLastPaceRatio(0.0);
            peakScores.put(driver, 0.0);
            lostChanceEmitted.put(driver, false);
        }

        state.setLastCompound(event.getCompound());
        state.setLastTyreLife(event.getTyreLife());

        Double lapTime = event.getLapTime();
        if (lapTime != null && lapTime > 0
                && event.getPitInTime() == null && event.getPitOutTime() == null
                && TrackStatusCodes.isGreenOrUnknown(event.getTrackStatus())) {
            if (lapTime < state.getStintBestLap()) {
                state.setStintBestLap(lapTime);
            }

            // pace ratio for continuous scoring
            if (state.getStintBestLap() > 0 && state.getStintBestLap() < Double.MAX_VALUE) {
                state.setLastPaceRatio((lapTime - state.getStintBestLap()) / state.getStintBestLap());
            }

            // track consecutive slow laps (still needed for filtering one-off blips)
            if (state.getLastPaceRatio() > SLOW_LAP_RATIO_THRESHOLD) {
                state.setConsecutiveSlowLaps(state.getConsecutiveSlowLaps() + 1);
            } else {
                state.setConsecutiveSlowLaps(0);
            }
        }

        driverStates.put(driver, state);
    }

    private List<LapEvent> collectFreshGrid(int leaderLap) throws Exception {
        List<LapEvent> currentGrid = new ArrayList<>();
        List<String> staleDrivers = new ArrayList<>();

        for (LapEvent e : latestGridState.values()) {
            if (e.getLapNumber() >= leaderLap - RECENT_LAP_WINDOW) {
                currentGrid.add(e);
            }
            if (e.getLapNumber() < leaderLap - RECENT_LAP_WINDOW) {
                staleDrivers.add(e.getDriver());
            }
        }

        for (String staleDriver : staleDrivers) {
            latestGridState.remove(staleDriver);
        }

        return currentGrid;
    }

    // evaluates the pit desirability score for each eligible driver
    private void evaluateAll(List<LapEvent> laps, String currentTrackStatus,
            Collector<PitSuggestionAlert> out) throws Exception {
        laps.sort(Comparator.comparingInt(LapEvent::getPosition));

        for (int i = 0; i < laps.size(); i++) {
            LapEvent current = laps.get(i);
            String driver = current.getDriver();

            // skip fresh tires, pit laps
            if (current.getTyreLife() < MIN_TYRE_LIFE) {
                continue;
            }
            if (current.getPitInTime() != null || current.getPitOutTime() != null) {
                continue;
            }

            double paceScore = computePaceScore(current);
            int trackStatusScore = computeTrackStatusScore(currentTrackStatus);
            TrafficResult traffic = computeTrafficResult(current, laps, i, currentTrackStatus);
            double strategyPenalty = computeStrategyPenalty(current);
            double urgencyScore = computeUrgencyScore(current);
            double endOfRacePenalty = computeEndOfRacePenalty(current);

            double totalScore = paceScore + trackStatusScore + traffic.score
                    + strategyPenalty + urgencyScore + endOfRacePenalty;
            totalScore = Math.max(0.0, Math.min(100.0, totalScore));

            // update peak score for lost_chance detection
            Double peak = peakScores.get(driver);
            if (peak == null) {
                peak = 0.0;
            }
            if (totalScore > peak) {
                peakScores.put(driver, totalScore);
                peak = totalScore;
            }

            // lost_chance detection: peak was >= 70 but score dropped below 40
            Boolean lostEmitted = lostChanceEmitted.get(driver);
            if (lostEmitted == null) {
                lostEmitted = false;
            }

            if (!lostEmitted && peak >= LOST_CHANCE_PEAK && totalScore < LOST_CHANCE_DROP) {
                DriverPitState ds = driverStates.get(driver);
                // only emit if degradation is still worsening (not improvement from new tires)
                if (ds != null && ds.getLastPaceRatio() > SLOW_LAP_RATIO_THRESHOLD) {
                    lostChanceEmitted.put(driver, true);
                    emitAlert(current, totalScore, paceScore, trackStatusScore,
                            traffic, strategyPenalty, urgencyScore, endOfRacePenalty,
                            currentTrackStatus, SuggestionLabel.LOST_CHANCE, out);
                    continue;
                }
            }

            if (totalScore < EMIT_THRESHOLD) {
                continue;
            }

            SuggestionLabel label = classifyScore(totalScore);

            // emit-gate: check if we should suppress this alert
            if (!shouldEmit(driver, current.getStint(), totalScore, currentTrackStatus, label)) {
                continue;
            }

            // update emit-gate tracking
            lastEmittedScore.put(driver, totalScore);
            lastEmittedStint.put(driver, current.getStint());
            lastEmittedTrackStatus.put(driver, currentTrackStatus);

            emitAlert(current, totalScore, paceScore, trackStatusScore,
                    traffic, strategyPenalty, urgencyScore, endOfRacePenalty,
                    currentTrackStatus, label, out);
        }
    }

    private void emitAlert(LapEvent current, double totalScore, double paceScore,
            int trackStatusScore, TrafficResult traffic, double strategyPenalty,
            double urgencyScore, double endOfRacePenalty, String trackStatus,
            SuggestionLabel label, Collector<PitSuggestionAlert> out) {

        String suggestion = buildSuggestion(paceScore, trackStatusScore,
                traffic.score, strategyPenalty, urgencyScore, endOfRacePenalty);

        out.collect(new PitSuggestionAlert(
                current.getDriver(),
                current.getLapNumber(),
                current.getPosition(),
                current.getCompound(),
                current.getTyreLife(),
                totalScore,
                paceScore,
                trackStatusScore,
                traffic.score,
                strategyPenalty,
                urgencyScore,
                endOfRacePenalty,
                trackStatus,
                traffic.emergencePosition,
                traffic.gapToPhysicalCar,
                label.name(),
                suggestion
        ));

        LOG.info("pit strategy: {} lap {} -> {} (score={})",
                current.getDriver(), current.getLapNumber(), label,
                String.format("%.1f", totalScore));
    }

    // emit-gate: suppresses re-emission unless score escalated or track status changed
    private boolean shouldEmit(String driver, int currentStint, double currentScore,
            String currentTrackStatus, SuggestionLabel currentLabel) throws Exception {
        Integer prevStint = lastEmittedStint.get(driver);
        Double prevScore = lastEmittedScore.get(driver);

        // first emission ever, or new stint -> always emit
        if (prevStint == null || prevStint != currentStint) {
            return true;
        }
        if (prevScore == null) {
            return true;
        }

        // class changed, always emit to keep dashboard and ml timeline aligned
        SuggestionLabel prevLabel = classifyScore(prevScore);
        if (currentLabel != prevLabel) {
            return true;
        }

        // track status changed -> re-emit (new opportunity)
        String prevTrackStatus = lastEmittedTrackStatus.get(driver);
        if (prevTrackStatus != null && !prevTrackStatus.equals(currentTrackStatus)) {
            return true;
        }

        // score moved enough in either direction, emit escalation or downgrade
        return Math.abs(currentScore - prevScore) >= RE_EMIT_DELTA;
    }

    // classifies continuous score into discrete label for pit wall decision
    private static SuggestionLabel classifyScore(double score) {
        if (score >= PIT_NOW_THRESHOLD) {
            return SuggestionLabel.PIT_NOW;
        }
        if (score >= GOOD_PIT_THRESHOLD) {
            return SuggestionLabel.GOOD_PIT;
        }
        return SuggestionLabel.MONITOR;
    }

    // continuous pace score: power 1.5 curve.
    // gentle at low degradation, aggressive at high.
    // ex: 1% deg -> 8.1, 2% deg -> 21.8, 3%+ -> 30.0
    private double computePaceScore(LapEvent current) throws Exception {
        DriverPitState state = driverStates.get(current.getDriver());
        if (state == null || current.getLapTime() == null) {
            return 0.0;
        }
        if (state.getStintBestLap() >= Double.MAX_VALUE) {
            return 0.0;
        }

        // require at least 1 consecutive slow lap to filter one-off blips
        if (state.getConsecutiveSlowLaps() < 1) {
            return 0.0;
        }

        double paceRatio = state.getLastPaceRatio();
        if (paceRatio <= 0) {
            return 0.0;
        }

        // 30 * min(1.0, (paceRatio / 0.03)^1.5)
        double normalized = paceRatio / PACE_CEILING_RATIO;
        return 30.0 * Math.min(1.0, Math.pow(normalized, PACE_CURVE_POWER));
    }

    // +60 if sc or vsc is active (crisp, binary event)
    private static int computeTrackStatusScore(String trackStatus) {
        if (trackStatus == null) {
            return 0;
        }
        return switch (trackStatus) {
            case TrackStatusCodes.SAFETY_CAR, TrackStatusCodes.VIRTUAL_SAFETY_CAR, TrackStatusCodes.VSC_ENDING ->
                TRACK_STATUS_SCORE;
            default ->
                0;
        };
    }

    // continuous traffic score: linear interpolation based on emergence gap.
    // >= 3.0s -> +30 (clean air)
    // 1.0-3.0s -> linear 0 to +30
    // 0.0-1.0s -> linear -30 to 0 (DRS danger zone)
    // < 0.0s -> -30 (stuck behind)
    // bonus +5 if car ahead has old tires (easy pass)
    private TrafficResult computeTrafficResult(
            LapEvent current,
            List<LapEvent> laps,
            int posIndex,
            String currentTrackStatus) {
        TrafficResult result = new TrafficResult();
        result.emergencePosition = current.getPosition();

        Double pitLoss = selectPitLoss(current, currentTrackStatus);
        if (pitLoss == null) {
            return result;
        }

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
                if (j == posIndex + 1) {
                    // gap behind > pitLoss, no positions lost
                    result.score = 30.0;
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
            result.score = 30.0;
            return result;
        }

        result.emergencePosition = physicalCarAhead.getPosition() + 1;
        result.gapToPhysicalCar = gapToPhysicalCar;

        // continuous gap-based scoring
        double emergenceGap = gapToPhysicalCar;

        if (emergenceGap >= CLEAN_AIR_GAP) {
            result.score = 30.0;
        } else if (emergenceGap >= DRS_THRESHOLD) {
            // linear interpolation: 1.0s -> 0, 3.0s -> +30
            result.score = 30.0 * (emergenceGap - DRS_THRESHOLD) / (CLEAN_AIR_GAP - DRS_THRESHOLD);
        } else if (emergenceGap >= 0) {
            // DRS danger zone: 0s -> -30, 1.0s -> 0
            result.score = -30.0 * (1.0 - emergenceGap / DRS_THRESHOLD);
        } else {
            result.score = -30.0;
        }

        // bonus for easy pass: car ahead on old tires is significantly slower
        if (physicalCarAhead.getTyreLife() >= EASY_PASS_TYRE_LIFE && result.score < 30.0) {
            result.score += EASY_PASS_BONUS;
            result.score = Math.min(30.0, result.score);
        }

        return result;
    }

    // continuous urgency score: quadratic ramp starting at 50% of max stint.
    // zero until tires are halfway through expected life, then accelerates.
    // ex: 70% stint -> 4.8, 90% stint -> 19.2, 100%+ -> 30.0
    private double computeUrgencyScore(LapEvent current) throws Exception {
        String compound = current.getCompound();
        int tyreAge = current.getTyreLife();

        Integer maxStint = null;
        if (compound != null) {
            maxStint = maxStintByCompound.get(compound);
        }
        if (maxStint == null) {
            maxStint = defaultMaxStint(compound);
        }

        double tyreRatio = (double) tyreAge / maxStint;

        if (tyreRatio < URGENCY_ONSET_RATIO) {
            return 0.0;
        }

        // 30 * min(1.0, ((tyreRatio - 0.5) / 0.5)^2)
        double normalized = (tyreRatio - URGENCY_ONSET_RATIO) / (1.0 - URGENCY_ONSET_RATIO);
        return 30.0 * Math.min(1.0, normalized * normalized);
    }

    // continuous strategy penalty: how much deficit vs needed stint on next compound.
    // if next compound can cover remaining laps, penalty = 0.
    // otherwise, scales linearly with deficit up to -15.
    private double computeStrategyPenalty(LapEvent current) throws Exception {
        int totalLaps = current.getTotalLaps();
        if (totalLaps <= 0) {
            return 0.0;
        }

        int lapsRemaining = totalLaps - current.getLapNumber();
        if (lapsRemaining <= 0) {
            return 0.0;
        }

        String nextCompound = inferNextCompound(current.getCompound());
        Integer maxStint = maxStintByCompound.get(nextCompound);
        if (maxStint == null) {
            maxStint = defaultMaxStint(nextCompound);
        }

        if (maxStint >= lapsRemaining) {
            return 0.0;
        }

        // deficit ratio: how much of the remaining distance can't be covered
        double deficit = (double) (lapsRemaining - maxStint) / lapsRemaining;
        return -15.0 * Math.min(1.0, deficit * 3.0);
    }

    // end-of-race suppression: logistic sigmoid that smoothly kills pit suggestions
    // in the final laps. the penalty is near-zero until ~85% race completion, then
    // ramps sharply through 90-95%, reaching -100 at 98%+.
    //
    // formula: -100 / (1 + e^(-k * (ratio - midpoint)))
    // with k=15, midpoint=0.92:
    //   85% -> -2.5, 90% -> -18.2, 92% -> -50.0, 95% -> -89.1, 98% -> -99.3
    //
    // this allows SC/VSC (+60) to still suggest pits at 90% (net +42 = MONITOR),
    // but correctly suppresses even SC-driven suggestions at 95%+ (net -29 = killed).
    private static double computeEndOfRacePenalty(LapEvent current) {
        int totalLaps = current.getTotalLaps();
        if (totalLaps <= 0) {
            return 0.0;
        }
        double raceCompletionRatio = (double) current.getLapNumber() / totalLaps;
        return -100.0 / (1.0 + Math.exp(-EOR_SIGMOID_K * (raceCompletionRatio - EOR_SIGMOID_MIDPOINT)));
    }

    // selects pit loss based on track status (green, sc, vsc)
    private static Double selectPitLoss(LapEvent lap, String currentTrackStatus) {
        String status = TrackStatusCodes.normalizeOrGreen(currentTrackStatus);
        return switch (status) {
            case TrackStatusCodes.GREEN ->
                lap.getPitLoss();
            case TrackStatusCodes.VIRTUAL_SAFETY_CAR, TrackStatusCodes.VSC_ENDING ->
                lap.getVscPitLoss();
            case TrackStatusCodes.SAFETY_CAR ->
                lap.getScPitLoss();
            default ->
                null;
        };
    }

    // builds human-readable explanation from active scoring components
    private static String buildSuggestion(double paceScore, int trackStatusScore,
            double trafficScore, double strategyPenalty, double urgencyScore,
            double endOfRacePenalty) {
        List<String> parts = new ArrayList<>();
        if (paceScore > 5.0) {
            parts.add("pace drop");
        }
        if (trackStatusScore > 0) {
            parts.add("SC/VSC opportunity");
        }
        if (trafficScore >= 25.0) {
            parts.add("clean air");
        } else if (trafficScore > 0) {
            parts.add("decent gap");
        } else if (trafficScore < -10.0) {
            parts.add("traffic risk");
        }
        if (strategyPenalty < -5.0) {
            parts.add("tight tire window");
        }
        if (urgencyScore >= 20.0) {
            parts.add("tire cliff");
        } else if (urgencyScore >= 10.0) {
            parts.add("closing window");
        }
        if (endOfRacePenalty < -30.0) {
            parts.add("race ending");
        }
        return parts.isEmpty() ? "general" : String.join(" + ", parts);
    }

    private static String inferNextCompound(String current) {
        if (current == null) {
            return "MEDIUM";
        }
        return switch (current.toUpperCase()) {
            case "SOFT" ->
                "MEDIUM";
            case "MEDIUM" ->
                "HARD";
            case "HARD" ->
                "MEDIUM";
            default ->
                "MEDIUM";
        };
    }

    private static int defaultMaxStint(String compound) {
        if (compound == null) {
            return DEFAULT_MEDIUM_STINT;
        }
        return switch (compound.toUpperCase()) {
            case "SOFT" ->
                DEFAULT_SOFT_STINT;
            case "MEDIUM" ->
                DEFAULT_MEDIUM_STINT;
            case "HARD" ->
                DEFAULT_HARD_STINT;
            case "INTERMEDIATE", "WET" ->
                DEFAULT_WET_STINT;
            default ->
                DEFAULT_MEDIUM_STINT;
        };
    }

    private static class TrafficResult {

        double score = 0;
        int emergencePosition = 0;
        double gapToPhysicalCar = 0;
    }
}
