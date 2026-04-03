package com.polimi.f1.operators.groundtruth;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polimi.f1.model.TrackStatusCodes;
import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.output.PitStopEvaluationAlert;
import com.polimi.f1.model.output.PitStopEvaluationAlert.Result;
import com.polimi.f1.state.groundtruth.CycleState;
import com.polimi.f1.state.groundtruth.PitCycle;
import com.polimi.f1.state.groundtruth.RivalSnapshot;

// classifies completed pit cycles using a state machine that tracks rival clusters,
// detects offset strategies (1-stop vs 2-stop), and normalizes gap deltas as percentages
// of the driver's baseline lap time for track-agnostic evaluation.
//
// keyed by constant "RACE" for global field visibility.
//
// state machine:
//   PENDING_SETTLE   -> driver just pitted, waiting for 4 completed green settle laps
//   PENDING_RIVAL    -> driver settled, waiting for net rivals to pit or offset timeout
//   (resolved)       -> cycle complete, emit classification and remove state
//
// rival cluster: captures car ahead (undercut target) and car behind (defense target)
// at pit entry. tracks whether each rival subsequently pits (stint change) to detect
// same-strategy vs offset-strategy scenarios.
//
// offset detection: if rival hasn't pitted after 40% of remaining race laps, confirms
// different strategy. gap trajectory at that point determines advantage/disadvantage.
//
// labels: SUCCESS_UNDERCUT, SUCCESS_OVERCUT, SUCCESS_DEFEND, SUCCESS_FREE_STOP,
//         OFFSET_ADVANTAGE, OFFSET_DISADVANTAGE, FAILURE_PACE_DEFICIT, FAILURE_TRAFFIC,
//         WEATHER_SURVIVAL_STOP, and granular unresolved reason codes
public class PitStopEvaluator
        extends KeyedProcessFunction<String, LapEvent, PitStopEvaluationAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(PitStopEvaluator.class);

    // required number of completed green laps after pit before evaluation
    private static final int SETTLE_LAPS = 4;
    private static final int WARMUP_LAPS = 2;

    // safety timeout: 15 minutes event time to clear stale records
    private static final long TIMER_TIMEOUT_MS = 900_000;

    private static final int RECENT_LAP_WINDOW = 3;
    private static final int RIVAL_LOOKBACK_EXTRA_LAPS = 2;
    private static final int GAP_SEARCH_WINDOW = 3;
    private static final int TIMER_FALLBACK_LOOKBACK = 5;
    private static final int LEADER_REFERENCE_SEARCH_WINDOW = 2;
    private static final int MIN_PACE_FALLBACK_SAMPLES = 2;
    private static final int PACE_FALLBACK_LAP_SPAN = 4;
    private static final double MIN_POSITIONAL_PRE_GAP_SECONDS = 0.3;
    private static final double TRAFFIC_GAP_THRESHOLD_SECONDS = 2.0;
    private static final int MAX_PLAUSIBLE_RIVAL_POSITION_DELTA = 6;
    private static final double MAX_PLAUSIBLE_GAP_SECONDS = 60.0;
    private static final double MAX_PLAUSIBLE_GAP_DELTA_SECONDS = 45.0;
    private static final double SAFE_BASELINE_LAP_TIME_SECONDS = 95.0;

    // percentage thresholds for gap classification (as % of baseline lap time)
    // ex: 0.5% of 80s baseline = 0.4s, of 105s baseline = 0.525s
    private static final double UNDERCUT_THRESHOLD_PCT = 0.5;
    private static final double DEFEND_BAND_PCT = 0.5;
    private static final double MAX_STRATEGIC_GAP_DELTA_PCT = 20.0;

    // free stop threshold: pit under sc/vsc with gap change < 1% of baseline
    private static final double FREE_STOP_THRESHOLD_PCT = 1.0;

    // fraction of remaining race distance before declaring offset strategy
    private static final double OFFSET_RACE_FRACTION = 0.4;

    // traffic detection: car ahead with tyreLife >= this is considered slow/passable,
    // but car ahead within this gap is considered blocking
    private static final int TRAFFIC_TYRE_LIFE_THRESHOLD = 25;
    private static final String RESOLUTION_INSUFFICIENT_DATA = "INSUFFICIENT_DATA";
    private static final String RESOLUTION_EARLY_LAP_FILTER = "EARLY_LAP_FILTER";
    private static final String RESOLUTION_REFERENCE_FALLBACK = "REFERENCE_FALLBACK";
    private static final String RESOLUTION_RIVAL_PIT_PACE_SHIFT = "RIVAL_PIT_PACE_SHIFT";
    private static final String RESOLUTION_SAFETY_TIMER_PACE_SHIFT = "SAFETY_TIMER_PACE_SHIFT";
    private static final String RESOLUTION_POSITIONAL_FALLBACK = "POSITIONAL_FALLBACK";

    // flat state: all lap events, key = "lapNumber:driver"
    private transient MapState<String, LapEvent> lapEvents;

    // pending pit cycles, keyed by driver abbreviation
    private transient MapState<String, PitCycle> pendingCycles;

    // latest observed lap per driver, used for bounded grid scans and stale eviction
    private transient MapState<String, Integer> driverLatestLap;

    // max observed lap in race, used as freshness reference
    private transient ValueState<Integer> globalMaxLap;

    // stint best per driver per stint, key = "driver:stint"
    private transient MapState<String, Double> stintBests;

    @Override
    public void open(OpenContext openContext) {
        // 2h ttl: prevents unbounded state growth over continuous streaming.
        // race data older than 2 hours is no longer relevant for pit cycle resolution.
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, LapEvent> lapDesc
                = new MapStateDescriptor<>("pit-eval-laps", String.class, LapEvent.class);
        lapDesc.enableTimeToLive(ttlConfig);
        lapEvents = getRuntimeContext().getMapState(lapDesc);

        MapStateDescriptor<String, PitCycle> cycleDesc
                = new MapStateDescriptor<>("pending-cycles", String.class, PitCycle.class);
        cycleDesc.enableTimeToLive(ttlConfig);
        pendingCycles = getRuntimeContext().getMapState(cycleDesc);

        MapStateDescriptor<String, Integer> latestLapDesc
                = new MapStateDescriptor<>("pit-eval-driver-latest-lap", String.class, Integer.class);
        latestLapDesc.enableTimeToLive(ttlConfig);
        driverLatestLap = getRuntimeContext().getMapState(latestLapDesc);

        ValueStateDescriptor<Integer> maxLapDesc
                = new ValueStateDescriptor<>("pit-eval-global-max-lap", Integer.class);
        maxLapDesc.enableTimeToLive(ttlConfig);
        globalMaxLap = getRuntimeContext().getState(maxLapDesc);

        MapStateDescriptor<String, Double> stintDesc
                = new MapStateDescriptor<>("stint-bests", String.class, Double.class);
        stintDesc.enableTimeToLive(ttlConfig);
        stintBests = getRuntimeContext().getMapState(stintDesc);
    }

    private static String lapKey(int lap, String driver) {
        return lap + ":" + driver;
    }

    private static String stintKey(String driver, int stint) {
        return driver + ":" + stint;
    }

    @Override
    public void processElement(LapEvent event, Context ctx, Collector<PitStopEvaluationAlert> out)
            throws Exception {
        if (event == null || event.getDriver() == null || event.getLapNumber() <= 0) {
            return;
        }

        int lap = event.getLapNumber();
        String driver = event.getDriver();

        lapEvents.put(lapKey(lap, driver), event);
        driverLatestLap.put(driver, lap);

        Integer maxLap = globalMaxLap.value();
        if (maxLap == null || lap > maxLap) {
            globalMaxLap.update(lap);
        }
        updateStintBest(event);

        // detect pit entry: pitInTime != null means this driver entered the pits this lap
        if (event.getPitInTime() != null && !pendingCycles.contains(driver)) {
            if (lap <= WARMUP_LAPS) {
                emitWarmupUnresolved(event, out);
            } else {
                registerPitCycle(event, ctx);
            }
        }

        // try to advance any pending cycles on every incoming event
        advanceCycles(event, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PitStopEvaluationAlert> out)
            throws Exception {
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, PitCycle> entry : pendingCycles.entries()) {
            PitCycle cycle = entry.getValue();
            if (cycle.getSafetyTimerTimestamp() <= timestamp) {
                LOG.info("safety timer: resolving {} pit on lap {}", cycle.getDriver(), cycle.getPitLap());
                resolveFromTimer(cycle, out);
                toRemove.add(entry.getKey());
            }
        }
        for (String key : toRemove) {
            pendingCycles.remove(key);
        }
    }

    // registers a new pit cycle when pit entry is detected
    private void registerPitCycle(LapEvent event, Context ctx) throws Exception {
        String driver = event.getDriver();
        int lap = event.getLapNumber();
        int position = event.getPosition();

        PitCycle cycle = new PitCycle();
        cycle.setDriver(driver);
        cycle.setPitLap(lap);
        cycle.setStintBeforePit(event.getStint());
        cycle.setTrackStatusAtPit(event.getTrackStatus());
        cycle.setTyreAgeAtPit(event.getTyreLife());
        cycle.setGapToCarAheadAtPit(event.getGapToCarAhead());
        cycle.setRace(event.getRace());
        cycle.setTotalLaps(event.getTotalLaps());
        cycle.setSettleLap(-1);
        cycle.setGreenLapsSincePit(0);
        cycle.setState(CycleState.PENDING_SETTLE);

        // capture baseline from pre-pit stint, with field-level and safe fallbacks.
        cycle.setBaselineLapTime(resolveBaselineLapTime(driver, event.getStint(), lap));

        // rival cluster: car ahead and car behind.
        // findDriverAtPosition falls back to lap-1 when sequential arrival
        // means the target position hasn't been stored yet for the current lap.
        RivalSnapshot ahead = findDriverAtPosition(position - 1, lap, driver);
        RivalSnapshot behind = findDriverAtPosition(position + 1, lap, driver);

        if (ahead != null) {
            cycle.setRivalAhead(ahead.getDriver());
            cycle.setRivalAheadStintAtPit(ahead.getStint());
            // compute gap on the lap where the rival was actually found
            cycle.setPrePitGapAhead(computeDirectedGap(driver, ahead.getDriver(), ahead.getFoundOnLap()));
            if (cycle.getPrePitGapAhead() == null) {
                cycle.setPrePitGapAhead(computeDirectedGap(driver, ahead.getDriver(), lap));
            }
        }
        if (behind != null) {
            cycle.setRivalBehind(behind.getDriver());
            cycle.setRivalBehindStintAtPit(behind.getStint());
            cycle.setPrePitGapBehind(computeDirectedGap(driver, behind.getDriver(), behind.getFoundOnLap()));
            if (cycle.getPrePitGapBehind() == null) {
                cycle.setPrePitGapBehind(computeDirectedGap(driver, behind.getDriver(), lap));
            }
        }

        // determine primary rival: car ahead for most drivers, car behind for P1
        if (position == 1 && behind != null) {
            cycle.setPrimaryRival(behind.getDriver());
            cycle.setPrePitGapToPrimary(cycle.getPrePitGapBehind());
            cycle.setPrimaryRivalStintAtPit(behind.getStint());
        } else if (ahead != null) {
            cycle.setPrimaryRival(ahead.getDriver());
            cycle.setPrePitGapToPrimary(cycle.getPrePitGapAhead());
            cycle.setPrimaryRivalStintAtPit(ahead.getStint());
        }

        // offset timeout: 40% of remaining race distance
        int remainingLaps = Math.max(1, cycle.getTotalLaps() - lap);
        cycle.setOffsetTimeoutLap(lap + (int) Math.ceil(remainingLaps * OFFSET_RACE_FRACTION));

        // safety timer
        cycle.setSafetyTimerTimestamp(event.getEventTimeMillis() + TIMER_TIMEOUT_MS);
        ctx.timerService().registerEventTimeTimer(cycle.getSafetyTimerTimestamp());

        // the compound after pit is unknown until the next lap's event arrives.
        // will be filled in during settle phase.
        cycle.setCompoundAfterPit(null);

        pendingCycles.put(driver, cycle);

        LOG.info("pit cycle registered: {} lap {} P{}, rivals: [ahead={}, behind={}], "
                + "baseline: {}, offset timeout lap: {}",
                driver, lap, position, cycle.getRivalAhead(), cycle.getRivalBehind(),
                cycle.getBaselineLapTime() > 0 ? String.format("%.3f", cycle.getBaselineLapTime()) : "N/A",
                cycle.getOffsetTimeoutLap());
    }

    // advances all pending cycles based on incoming events
    private void advanceCycles(LapEvent trigger, Collector<PitStopEvaluationAlert> out)
            throws Exception {
        int currentLap = trigger.getLapNumber();
        List<String> resolved = new ArrayList<>();

        for (Map.Entry<String, PitCycle> entry : pendingCycles.entries()) {
            PitCycle cycle = entry.getValue();

            // fill compound after pit from first post-pit lap
            if (cycle.getCompoundAfterPit() == null
                    && trigger.getDriver().equals(cycle.getDriver())
                    && trigger.getLapNumber() > cycle.getPitLap()) {
                cycle.setCompoundAfterPit(trigger.getCompound());
                pendingCycles.put(entry.getKey(), cycle);
            }

            if (cycle.getState() == CycleState.PENDING_SETTLE) {
                if (trigger.getDriver().equals(cycle.getDriver())
                        && trigger.getLapNumber() > cycle.getPitLap()) {
                    String status = trigger.getTrackStatus();
                    if (TrackStatusCodes.isGreenOrUnknown(status)) {
                        cycle.setGreenLapsSincePit(cycle.getGreenLapsSincePit() + 1);
                    }

                    if (cycle.getGreenLapsSincePit() >= SETTLE_LAPS) {
                        cycle.setSettleLap(trigger.getLapNumber());
                        cycle.setState(CycleState.PENDING_RIVAL);
                        pendingCycles.put(entry.getKey(), cycle);
                        if (tryResolveRival(cycle, currentLap, out)) {
                            resolved.add(entry.getKey());
                        }
                    } else {
                        pendingCycles.put(entry.getKey(), cycle);
                    }
                }
            } else if (cycle.getState() == CycleState.PENDING_RIVAL) {
                if (tryResolveRival(cycle, currentLap, out)) {
                    resolved.add(entry.getKey());
                }
            }
        }

        for (String key : resolved) {
            pendingCycles.remove(key);
        }
    }

    // tries to resolve a cycle in PENDING_RIVAL state.
    // checks if primary rival has pitted (same strategy) or if offset timeout reached.
    // returns true if resolved.
    private boolean tryResolveRival(PitCycle cycle, int currentLap,
            Collector<PitStopEvaluationAlert> out) throws Exception {

        if (!ensurePrimaryRivalContext(cycle)) {
            return tryResolveWithReferenceFallback(cycle, currentLap, out);
        }

        double baselineLap = ensureValidBaseline(cycle, cycle.getPitLap());
        if (baselineLap <= 0) {
            emitResult(cycle, null, null, Result.UNRESOLVED_INVALID_BASELINE,
                RESOLUTION_INSUFFICIENT_DATA, false, out);
            return true;
        }

        // check if primary rival has pitted: either after our pit (undercut scenario)
        // or shortly before our pit (overcut scenario, rival already on new tires)
        boolean rivalPittedAfter = hasRivalPitted(cycle.getPrimaryRival(), cycle.getPrimaryRivalStintAtPit(),
                cycle.getPitLap(), currentLap);

        // overcut check: rival pitted in the window before our pit (within SETTLE_LAPS + 2 laps)
        int lookbackStart = Math.max(1, cycle.getPitLap() - SETTLE_LAPS - RIVAL_LOOKBACK_EXTRA_LAPS);
        boolean rivalPittedBefore = hasRivalPittedInRange(cycle.getPrimaryRival(),
                lookbackStart, cycle.getPitLap());

        boolean rivalPitted = rivalPittedAfter || rivalPittedBefore;

        if (rivalPitted) {
            int rivalPitLap;
            if (rivalPittedBefore) {
                rivalPitLap = findRivalPitLapInRange(cycle.getPrimaryRival(), lookbackStart, cycle.getPitLap());
            } else {
                rivalPitLap = findRivalPitLap(cycle.getPrimaryRival(), cycle.getPrimaryRivalStintAtPit(),
                        cycle.getPitLap(), currentLap);
            }
            cycle.setDriverPittedFirst(rivalPitLap < 0 || cycle.getPitLap() <= rivalPitLap);

            // wait for rival to also complete settle_laps green laps before comparing gaps
            int rivalSettleLap = calculateRivalSettleLap(cycle.getPrimaryRival(), rivalPitLap, currentLap);
            if (rivalSettleLap < 0) {
                return false;
            }
            int comparisonLap = Math.max(cycle.getSettleLap(), rivalSettleLap);

            Double postGap = findGapNearLap(cycle.getDriver(), cycle.getPrimaryRival(), comparisonLap);
            if (postGap == null && currentLap >= comparisonLap + RIVAL_LOOKBACK_EXTRA_LAPS) {
                postGap = findGapNearLap(cycle.getDriver(), cycle.getPrimaryRival(), cycle.getSettleLap());
            }
            if (postGap == null && currentLap < comparisonLap + GAP_SEARCH_WINDOW) {
                return false; // wait for more data
            }
            if (postGap == null) {
                if (tryResolveWithPaceShiftFallback(cycle, comparisonLap,
                        baselineLap, RESOLUTION_RIVAL_PIT_PACE_SHIFT, out)) {
                    return true;
                }
                // this reasoning is from garcia tejada 2023, use ordinal rival order when gap chain is missing.
                if (tryResolveWithPositionalFallback(cycle, comparisonLap, false, out)) {
                    return true;
                }
                emitResult(cycle, null, null, Result.UNRESOLVED_MISSING_POST_GAP,
                    RESOLUTION_INSUFFICIENT_DATA, false, out);
                return true;
            }

            boolean emergenceTraffic = checkEmergenceTraffic(cycle.getDriver(), cycle.getSettleLap());

            Result result = classifyPitStop(cycle.getPrePitGapToPrimary(), postGap,
                    baselineLap, cycle.isDriverPittedFirst(),
                    cycle.getTrackStatusAtPit(), cycle.getCompoundAfterPit(), emergenceTraffic);

            Double gapDeltaPct = computeGapDeltaPct(cycle.getPrePitGapToPrimary(), postGap,
                    baselineLap);

            emitResult(cycle, postGap, gapDeltaPct, result, "RIVAL_PIT", false, out);
            return true;
        }

        // check offset timeout: rival hasn't pitted after 40% of remaining race
        if (currentLap >= cycle.getOffsetTimeoutLap()) {
            Double currentGap = findGapNearLap(cycle.getDriver(), cycle.getPrimaryRival(), currentLap);
            if (currentGap == null) {
                currentGap = findGapNearLap(cycle.getDriver(), cycle.getPrimaryRival(), currentLap - 1);
            }

            Double gapDeltaPct = computeGapDeltaPct(cycle.getPrePitGapToPrimary(), currentGap,
                    baselineLap);
            Result result;
            if (gapDeltaPct == null) {
                result = Result.UNRESOLVED_MISSING_POST_GAP;
            } else if (isIncidentGapDeltaPct(gapDeltaPct)) {
                result = Result.UNRESOLVED_INCIDENT_FILTER;
            } else if (gapDeltaPct < -DEFEND_BAND_PCT) {
                result = Result.OFFSET_ADVANTAGE;
            } else if (gapDeltaPct > DEFEND_BAND_PCT) {
                result = Result.OFFSET_DISADVANTAGE;
            } else {
                result = Result.SUCCESS_DEFEND;
            }

            emitResult(cycle, currentGap, gapDeltaPct, result, "OFFSET_TIMEOUT", true, out);
            return true;
        }

        return false;
    }

    // same-strategy classification based on gap delta percentage
    private Result classifyPitStop(Double prePitGap, Double postPitGap,
            double baselineLap, boolean driverPittedFirst,
            String trackStatus, String compoundAfterPit, boolean emergenceTraffic) {

        if (isWetCompound(compoundAfterPit)) {
            return Result.WEATHER_SURVIVAL_STOP;
        }

        if (prePitGap == null) {
            return Result.UNRESOLVED_MISSING_PRE_GAP;
        }
        if (postPitGap == null) {
            return Result.UNRESOLVED_MISSING_POST_GAP;
        }
        if (baselineLap <= 0) {
            return Result.UNRESOLVED_INVALID_BASELINE;
        }

        Double gapDeltaPct = computeGapDeltaPct(prePitGap, postPitGap, baselineLap);
        if (gapDeltaPct == null) {
            return Result.UNRESOLVED_MISSING_POST_GAP;
        }
        if (isIncidentGapDeltaPct(gapDeltaPct)) {
            return Result.UNRESOLVED_INCIDENT_FILTER;
        }

        // sc/vsc free stop: pitted under caution with minimal gap distortion
        if (TrackStatusCodes.isCaution(trackStatus)) {
            if (Math.abs(gapDeltaPct) < FREE_STOP_THRESHOLD_PCT) {
                return Result.SUCCESS_FREE_STOP;
            }
        }

        // same-strategy classification
        if (gapDeltaPct < -UNDERCUT_THRESHOLD_PCT) {
            return driverPittedFirst ? Result.SUCCESS_UNDERCUT : Result.SUCCESS_OVERCUT;
        }

        if (Math.abs(gapDeltaPct) <= DEFEND_BAND_PCT) {
            if (emergenceTraffic) {
                return Result.FAILURE_TRAFFIC;
            }
            return Result.SUCCESS_DEFEND;
        }

        // gapDeltaPct > DEFEND_BAND_PCT: lost ground
        if (emergenceTraffic) {
            return Result.FAILURE_TRAFFIC;
        }
        return Result.FAILURE_PACE_DEFICIT;
    }

    private Result classifyReferenceFallback(String trackStatus, String compoundAfterPit, Double gapDeltaPct) {
        if (isWetCompound(compoundAfterPit)) {
            return Result.WEATHER_SURVIVAL_STOP;
        }
        if (gapDeltaPct == null) {
            return Result.UNRESOLVED_MISSING_POST_GAP;
        }
        if (isIncidentGapDeltaPct(gapDeltaPct)) {
            return Result.UNRESOLVED_INCIDENT_FILTER;
        }
        if (TrackStatusCodes.isCaution(trackStatus) && Math.abs(gapDeltaPct) < FREE_STOP_THRESHOLD_PCT) {
            return Result.SUCCESS_FREE_STOP;
        }
        if (gapDeltaPct <= DEFEND_BAND_PCT) {
            return Result.SUCCESS_DEFEND;
        }
        return Result.FAILURE_PACE_DEFICIT;
    }

    // ensures the cycle has a comparable rival and a valid pre-pit directed gap.
    // if the original primary rival is unusable, retry with ahead/behind candidates.
    private boolean ensurePrimaryRivalContext(PitCycle cycle) throws Exception {
        if (cycle.getPrimaryRival() != null) {
            Double preGap = cycle.getPrePitGapToPrimary();
            if (preGap == null) {
                preGap = findGapNearLap(cycle.getDriver(), cycle.getPrimaryRival(), cycle.getPitLap());
                cycle.setPrePitGapToPrimary(preGap);
            }
            if (preGap != null) {
                return true;
            }
        }

        if (tryAssignPrimaryRival(cycle, cycle.getRivalAhead(),
                cycle.getPrePitGapAhead(), cycle.getRivalAheadStintAtPit())) {
            return true;
        }

        return tryAssignPrimaryRival(cycle, cycle.getRivalBehind(),
                cycle.getPrePitGapBehind(), cycle.getRivalBehindStintAtPit());
    }

    private boolean tryAssignPrimaryRival(PitCycle cycle, String candidateRival,
            Double candidateGap, int candidateStintAtPit) throws Exception {
        if (candidateRival == null) {
            return false;
        }

        Double preGap = candidateGap;
        if (preGap == null) {
            preGap = findGapNearLap(cycle.getDriver(), candidateRival, cycle.getPitLap());
        }
        if (preGap == null) {
            return false;
        }

        cycle.setPrimaryRival(candidateRival);
        cycle.setPrimaryRivalStintAtPit(candidateStintAtPit);
        cycle.setPrePitGapToPrimary(preGap);
        return true;
    }

    // fallback path when both direct rival resolution and directed gaps are unavailable.
    // computes pre/post pace-relative gap against race leader lap time, and if unavailable,
    // against the driver's own recent moving average.
    private boolean tryResolveWithReferenceFallback(PitCycle cycle, int currentLap,
            Collector<PitStopEvaluationAlert> out) throws Exception {
        int comparisonLap = cycle.getSettleLap() > 0
                ? cycle.getSettleLap()
                : cycle.getPitLap() + SETTLE_LAPS;

        if (currentLap < comparisonLap) {
            return false;
        }

        double baselineLap = ensureValidBaseline(cycle, cycle.getPitLap());
        if (baselineLap <= 0) {
            emitResult(cycle, null, null, Result.UNRESOLVED_INVALID_BASELINE,
                    RESOLUTION_INSUFFICIENT_DATA, false, out);
            return true;
        }

        int preAnchorLap = Math.max(1, cycle.getPitLap() - 1);
        Double preReferenceGap = computeReferencePaceGap(cycle.getDriver(), preAnchorLap);
        if (preReferenceGap == null) {
            boolean hasRivalIdentity = cycle.getPrimaryRival() != null
                || cycle.getRivalAhead() != null
                || cycle.getRivalBehind() != null;
            Result unresolved = hasRivalIdentity
                ? Result.UNRESOLVED_MISSING_PRE_GAP
                : Result.UNRESOLVED_MISSING_RIVAL;
            emitResult(cycle, null, null, unresolved, RESOLUTION_INSUFFICIENT_DATA, false, out);
            return true;
        }

        Double postReferenceGap = computeReferencePaceGap(cycle.getDriver(), comparisonLap);
        if (postReferenceGap == null && currentLap < comparisonLap + GAP_SEARCH_WINDOW) {
            return false;
        }
        if (postReferenceGap == null) {
            emitResult(cycle, null, null, Result.UNRESOLVED_MISSING_POST_GAP,
                    RESOLUTION_INSUFFICIENT_DATA, false, out);
            return true;
        }

        Double gapDeltaPct = computeGapDeltaPct(preReferenceGap, postReferenceGap, baselineLap);
        Result result = classifyReferenceFallback(
                cycle.getTrackStatusAtPit(), cycle.getCompoundAfterPit(), gapDeltaPct);

        emitResult(cycle, postReferenceGap, gapDeltaPct, result,
                RESOLUTION_REFERENCE_FALLBACK, false, out);
        return true;
    }

    private Double computeReferencePaceGap(String driver, int anchorLap) throws Exception {
        if (anchorLap <= 0) {
            return null;
        }

        Double driverPace = computeDriverMovingAverageLapTime(driver, anchorLap, RECENT_LAP_WINDOW);
        if (driverPace == null) {
            return null;
        }

        Double referencePace = findReferenceLapTime(driver, anchorLap);
        if (referencePace == null || referencePace <= 0) {
            return null;
        }

        return driverPace - referencePace;
    }

    // conservative fallback for missing post-gap after rival pit.
    // uses relative lap-time shift (driver minus rival) before vs after pit cycle.
    // only used when direct positional gap reconstruction is unavailable.
    private boolean tryResolveWithPaceShiftFallback(PitCycle cycle, int comparisonLap,
            double baselineLap, String resolvedVia,
            Collector<PitStopEvaluationAlert> out) throws Exception {
        if (cycle.getPrimaryRival() == null) {
            return false;
        }

        // this reasoning is from heilmeier 2020, compare relative pace shift pre pit vs post pit.
        int preAnchorLap = Math.max(1, cycle.getPitLap() - 1);
        Double prePaceGap = findRelativePaceGapNearLap(
                cycle.getDriver(), cycle.getPrimaryRival(), preAnchorLap, MIN_PACE_FALLBACK_SAMPLES);
        if (prePaceGap == null) {
            return false;
        }

        Double postPaceGap = findRelativePaceGapNearLap(
                cycle.getDriver(), cycle.getPrimaryRival(), comparisonLap, MIN_PACE_FALLBACK_SAMPLES);
        if (postPaceGap == null) {
            return false;
        }

        Double gapDeltaPct = computeGapDeltaPct(prePaceGap, postPaceGap, baselineLap);
        if (gapDeltaPct == null) {
            return false;
        }

        int settleLap = cycle.getSettleLap() > 0 ? cycle.getSettleLap() : comparisonLap;
        boolean emergenceTraffic = checkEmergenceTraffic(cycle.getDriver(), settleLap);

        Result result = classifyPitStop(prePaceGap, postPaceGap,
                baselineLap, cycle.isDriverPittedFirst(),
                cycle.getTrackStatusAtPit(), cycle.getCompoundAfterPit(), emergenceTraffic);

        emitResult(cycle, postPaceGap, gapDeltaPct, result, resolvedVia, false, out);
        return true;
    }

    // ordinal fallback for cases where directed gap reconstruction and pace-shift are unavailable.
    // resolves only clear order flips against the primary rival, ambiguous order is kept unresolved.
    private boolean tryResolveWithPositionalFallback(PitCycle cycle, int comparisonLap,
            boolean requireRivalPitEvidence,
            Collector<PitStopEvaluationAlert> out) throws Exception {
        // this reasoning is from garcia tejada 2023, position maintained or improved is a valid fallback target.
        if (cycle.getPrimaryRival() == null || cycle.getPrePitGapToPrimary() == null) {
            return false;
        }
        if (Math.abs(cycle.getPrePitGapToPrimary()) < MIN_POSITIONAL_PRE_GAP_SECONDS) {
            return false;
        }
        if (requireRivalPitEvidence && !hasRivalPitEvidence(cycle, comparisonLap)) {
            return false;
        }

        LapEvent driverLap = findDriverEventNearLap(cycle.getDriver(), comparisonLap);
        LapEvent rivalLap = findDriverEventNearLap(cycle.getPrimaryRival(), comparisonLap);
        if (driverLap == null || rivalLap == null) {
            return false;
        }
        if (!TrackStatusCodes.isGreenOrUnknown(driverLap.getTrackStatus())
                || !TrackStatusCodes.isGreenOrUnknown(rivalLap.getTrackStatus())) {
            return false;
        }
        if (Math.abs(driverLap.getLapNumber() - rivalLap.getLapNumber()) > 1) {
            return false;
        }

        int driverPos = driverLap.getPosition();
        int rivalPos = rivalLap.getPosition();
        if (driverPos <= 0 || rivalPos <= 0) {
            return false;
        }
        if (Math.abs(driverPos - rivalPos) > MAX_PLAUSIBLE_RIVAL_POSITION_DELTA) {
            return false;
        }

        boolean driverAheadPrePit = cycle.getPrePitGapToPrimary() < 0;
        boolean driverAheadPostPit = driverPos < rivalPos;
        if (driverAheadPrePit == driverAheadPostPit) {
            return false;
        }

        Result result;
        if (!driverAheadPrePit && driverAheadPostPit) {
            result = cycle.isDriverPittedFirst() ? Result.SUCCESS_UNDERCUT : Result.SUCCESS_OVERCUT;
        } else {
            int settleLap = cycle.getSettleLap() > 0 ? cycle.getSettleLap() : comparisonLap;
            boolean emergenceTraffic = checkEmergenceTraffic(cycle.getDriver(), settleLap);
            result = emergenceTraffic ? Result.FAILURE_TRAFFIC : Result.FAILURE_PACE_DEFICIT;
        }

        emitResult(cycle, null, null, result, RESOLUTION_POSITIONAL_FALLBACK, false, out);
        return true;
    }

    private boolean hasRivalPitEvidence(PitCycle cycle, int comparisonLap) throws Exception {
        if (cycle.getPrimaryRival() == null) {
            return false;
        }

        // this reasoning is from carrasco heine and thraves 2023, compare rivals only when both completed pit sequence.
        int toLap = Math.max(cycle.getPitLap(), comparisonLap + GAP_SEARCH_WINDOW);
        if (hasRivalPitted(cycle.getPrimaryRival(), cycle.getPrimaryRivalStintAtPit(),
                cycle.getPitLap(), toLap)) {
            return true;
        }

        int lookbackStart = Math.max(1, cycle.getPitLap() - SETTLE_LAPS - RIVAL_LOOKBACK_EXTRA_LAPS);
        return hasRivalPittedInRange(cycle.getPrimaryRival(), lookbackStart, cycle.getPitLap());
    }

    private LapEvent findDriverEventNearLap(String driver, int targetLap) throws Exception {
        if (targetLap <= 0) {
            return null;
        }

        LapEvent event = lapEvents.get(lapKey(targetLap, driver));
        if (event != null) {
            return event;
        }

        for (int offset = 1; offset <= GAP_SEARCH_WINDOW; offset++) {
            int beforeLap = targetLap - offset;
            if (beforeLap > 0) {
                event = lapEvents.get(lapKey(beforeLap, driver));
                if (event != null) {
                    return event;
                }
            }

            event = lapEvents.get(lapKey(targetLap + offset, driver));
            if (event != null) {
                return event;
            }
        }

        return null;
    }

    private Double findRelativePaceGapNearLap(String driver, String rival,
            int targetLap, int minSamples) throws Exception {
        Double paceGap = computeRelativePaceGap(driver, rival, targetLap, minSamples);
        if (paceGap != null) {
            return paceGap;
        }

        for (int offset = 1; offset <= GAP_SEARCH_WINDOW; offset++) {
            paceGap = computeRelativePaceGap(driver, rival, targetLap - offset, minSamples);
            if (paceGap != null) {
                return paceGap;
            }

            paceGap = computeRelativePaceGap(driver, rival, targetLap + offset, minSamples);
            if (paceGap != null) {
                return paceGap;
            }
        }

        return null;
    }

    private Double computeRelativePaceGap(String driver, String rival,
            int anchorLap, int minSamples) throws Exception {
        if (anchorLap <= 0) {
            return null;
        }

        Double driverPace = computeDriverLocalAverageLapTime(
                driver, anchorLap, PACE_FALLBACK_LAP_SPAN, minSamples);
        if (driverPace == null) {
            return null;
        }

        Double rivalPace = computeDriverLocalAverageLapTime(
                rival, anchorLap, PACE_FALLBACK_LAP_SPAN, minSamples);
        if (rivalPace == null) {
            return null;
        }

        return driverPace - rivalPace;
    }

    private Double computeDriverLocalAverageLapTime(String driver, int endLap,
            int lapSpan, int minSamples) throws Exception {
        if (endLap <= 0 || lapSpan <= 0 || minSamples <= 0) {
            return null;
        }

        int used = 0;
        double sum = 0.0;
        int startLap = Math.max(1, endLap - lapSpan + 1);
        for (int lap = endLap; lap >= startLap; lap--) {
            LapEvent event = lapEvents.get(lapKey(lap, driver));
            if (event == null) {
                continue;
            }
            if (event.getLapTime() == null || event.getLapTime() <= 0) {
                continue;
            }
            if (event.getPitInTime() != null || event.getPitOutTime() != null) {
                continue;
            }
            if (!TrackStatusCodes.isGreenOrUnknown(event.getTrackStatus())) {
                continue;
            }

            sum += event.getLapTime();
            used++;
        }

        if (used < minSamples) {
            return null;
        }
        return sum / used;
    }

    private Double findReferenceLapTime(String driver, int lap) throws Exception {
        for (int offset = 0; offset <= LEADER_REFERENCE_SEARCH_WINDOW; offset++) {
            int candidateLap = lap - offset;
            if (candidateLap <= 0) {
                break;
            }
            Double leaderLap = findLeaderLapTime(candidateLap);
            if (leaderLap != null && leaderLap > 0) {
                return leaderLap;
            }
        }

        return computeDriverMovingAverageLapTime(driver, lap - 1, RECENT_LAP_WINDOW);
    }

    private Double findLeaderLapTime(int lap) throws Exception {
        Integer maxLap = globalMaxLap.value();
        if (maxLap == null) {
            maxLap = lap;
        }

        List<String> staleDrivers = new ArrayList<>();
        for (String d : driverLatestLap.keys()) {
            Integer latestLap = driverLatestLap.get(d);
            if (latestLap == null || maxLap - latestLap > RECENT_LAP_WINDOW) {
                staleDrivers.add(d);
                continue;
            }

            LapEvent e = lapEvents.get(lapKey(lap, d));
            if (e == null || e.getPosition() != 1) {
                continue;
            }
            if (e.getLapTime() == null || e.getLapTime() <= 0) {
                continue;
            }
            if (e.getPitInTime() != null || e.getPitOutTime() != null) {
                continue;
            }
            if (!TrackStatusCodes.isGreenOrUnknown(e.getTrackStatus())) {
                continue;
            }

            for (String stale : staleDrivers) {
                driverLatestLap.remove(stale);
            }
            return e.getLapTime();
        }

        for (String stale : staleDrivers) {
            driverLatestLap.remove(stale);
        }
        return null;
    }

    private Double computeDriverMovingAverageLapTime(String driver, int endLap, int window)
            throws Exception {
        return computeDriverMovingAverageLapTime(driver, endLap, window, 1);
    }

    private Double computeDriverMovingAverageLapTime(String driver, int endLap,
            int window, int minSamples) throws Exception {
        if (endLap <= 0) {
            return null;
        }

        int used = 0;
        double sum = 0.0;
        for (int lap = endLap; lap >= 1 && used < window; lap--) {
            LapEvent event = lapEvents.get(lapKey(lap, driver));
            if (event == null) {
                continue;
            }
            if (event.getLapTime() == null || event.getLapTime() <= 0) {
                continue;
            }
            if (event.getPitInTime() != null || event.getPitOutTime() != null) {
                continue;
            }
            if (!TrackStatusCodes.isGreenOrUnknown(event.getTrackStatus())) {
                continue;
            }

            sum += event.getLapTime();
            used++;
        }

        if (used < minSamples) {
            return null;
        }
        return sum / used;
    }

    private double ensureValidBaseline(PitCycle cycle, int referenceLap) throws Exception {
        if (cycle.getBaselineLapTime() > 0) {
            return cycle.getBaselineLapTime();
        }

        Double fieldMedian = computeFieldMedianLapTime(referenceLap);
        if (fieldMedian != null && fieldMedian > 0) {
            cycle.setBaselineLapTime(fieldMedian);
            return fieldMedian;
        }

        if (SAFE_BASELINE_LAP_TIME_SECONDS > 0) {
            cycle.setBaselineLapTime(SAFE_BASELINE_LAP_TIME_SECONDS);
            return SAFE_BASELINE_LAP_TIME_SECONDS;
        }

        return 0.0;
    }

    private double resolveBaselineLapTime(String driver, int stintBeforePit, int pitLap) throws Exception {
        String sKey = stintKey(driver, stintBeforePit);
        Double best = stintBests.get(sKey);
        if (best != null && best > 0) {
            return best;
        }

        Double fieldMedian = computeFieldMedianLapTime(pitLap);
        if (fieldMedian != null && fieldMedian > 0) {
            return fieldMedian;
        }

        return SAFE_BASELINE_LAP_TIME_SECONDS;
    }

    private Double computeFieldMedianLapTime(int lap) throws Exception {
        Integer maxLap = globalMaxLap.value();
        if (maxLap == null) {
            maxLap = lap;
        }

        List<String> staleDrivers = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        for (String d : driverLatestLap.keys()) {
            Integer latestLap = driverLatestLap.get(d);
            if (latestLap == null || maxLap - latestLap > RECENT_LAP_WINDOW) {
                staleDrivers.add(d);
                continue;
            }

            LapEvent event = lapEvents.get(lapKey(lap, d));
            if (event == null) {
                continue;
            }
            if (event.getLapTime() == null || event.getLapTime() <= 0) {
                continue;
            }
            if (event.getPitInTime() != null || event.getPitOutTime() != null) {
                continue;
            }
            if (!TrackStatusCodes.isGreenOrUnknown(event.getTrackStatus())) {
                continue;
            }

            values.add(event.getLapTime());
        }

        for (String stale : staleDrivers) {
            driverLatestLap.remove(stale);
        }

        if (values.isEmpty()) {
            return null;
        }

        Collections.sort(values);
        int n = values.size();
        if ((n % 2) == 1) {
            return values.get(n / 2);
        }
        return (values.get((n / 2) - 1) + values.get(n / 2)) / 2.0;
    }

    // computes gap change as % of baseline lap time.
    // ex: prePitGap=2.5, postPitGap=1.0, baseline=82.0 -> (1.0-2.5)/82.0*100 = -1.83%
    private static Double computeGapDeltaPct(Double prePitGap, Double postPitGap, double baseline) {
        if (prePitGap == null || postPitGap == null || baseline <= 0) {
            return null;
        }
        if (!isPlausibleGap(prePitGap) || !isPlausibleGap(postPitGap)) {
            return null;
        }
        double deltaSeconds = postPitGap - prePitGap;
        if (Math.abs(deltaSeconds) > MAX_PLAUSIBLE_GAP_DELTA_SECONDS) {
            return null;
        }
        return (postPitGap - prePitGap) / baseline * 100.0;
    }

    private static boolean isPlausibleGap(Double gap) {
        return gap != null && Math.abs(gap) <= MAX_PLAUSIBLE_GAP_SECONDS;
    }

    private static boolean isWetCompound(String compound) {
        if (compound == null) {
            return false;
        }
        return "INTERMEDIATE".equalsIgnoreCase(compound) || "WET".equalsIgnoreCase(compound);
    }

    private static boolean isIncidentGapDeltaPct(Double gapDeltaPct) {
        return gapDeltaPct != null && Math.abs(gapDeltaPct) > MAX_STRATEGIC_GAP_DELTA_PCT;
    }

    // checks if the rival's stint changed between pitLap and currentLap
    private boolean hasRivalPitted(String rival, int stintAtDriverPit,
            int fromLap, int toLap) throws Exception {
        for (int lap = fromLap + 1; lap <= toLap; lap++) {
            LapEvent rivalLap = lapEvents.get(lapKey(lap, rival));
            if (rivalLap != null && rivalLap.getStint() > stintAtDriverPit) {
                return true;
            }
            if (rivalLap != null && rivalLap.getPitInTime() != null) {
                return true;
            }
        }
        return false;
    }

    // checks if the rival pitted anywhere in a lap range (for overcut detection)
    private boolean hasRivalPittedInRange(String rival, int fromLap, int toLap) throws Exception {
        for (int lap = fromLap; lap <= toLap; lap++) {
            LapEvent rivalLap = lapEvents.get(lapKey(lap, rival));
            if (rivalLap != null && rivalLap.getPitInTime() != null) {
                return true;
            }
        }
        return false;
    }

    // finds the exact lap where the rival pitted within a range (for overcut detection)
    private int findRivalPitLapInRange(String rival, int fromLap, int toLap) throws Exception {
        for (int lap = fromLap; lap <= toLap; lap++) {
            LapEvent rivalLap = lapEvents.get(lapKey(lap, rival));
            if (rivalLap != null && rivalLap.getPitInTime() != null) {
                return lap;
            }
        }
        return -1;
    }

    // finds the lap where the rival actually pitted (stint changed or pitInTime detected)
    private int findRivalPitLap(String rival, int stintAtDriverPit,
            int fromLap, int toLap) throws Exception {
        for (int lap = fromLap + 1; lap <= toLap; lap++) {
            LapEvent rivalLap = lapEvents.get(lapKey(lap, rival));
            if (rivalLap != null && rivalLap.getPitInTime() != null) {
                return lap;
            }
            if (rivalLap != null && rivalLap.getStint() > stintAtDriverPit) {
                return lap;
            }
        }
        return -1;
    }

    // calculates the rival settle lap as the lap where rival reaches settle_laps green laps after pit
    private int calculateRivalSettleLap(String rival, int rivalPitLap, int currentLap) throws Exception {
        if (rivalPitLap < 0) {
            return -1;
        }

        int greenLaps = 0;
        for (int l = rivalPitLap + 1; l <= currentLap; l++) {
            LapEvent e = lapEvents.get(lapKey(l, rival));
            if (e != null && TrackStatusCodes.isGreenOrUnknown(e.getTrackStatus())) {
                greenLaps++;
                if (greenLaps >= SETTLE_LAPS) {
                    return l;
                }
            }
        }

        return -1;
    }

    // searches for gap data near the target lap, scanning +-3 laps for missing data
    private Double findGapNearLap(String driverA, String driverB, int targetLap) throws Exception {
        Double gap = computeDirectedGap(driverA, driverB, targetLap);
        if (gap != null) {
            return gap;
        }
        for (int offset = 1; offset <= GAP_SEARCH_WINDOW; offset++) {
            gap = computeDirectedGap(driverA, driverB, targetLap - offset);
            if (gap != null) {
                return gap;
            }
            gap = computeDirectedGap(driverA, driverB, targetLap + offset);
            if (gap != null) {
                return gap;
            }
        }
        return null;
    }

    // checks if the driver emerged into traffic at the settle lap.
    // traffic = car ahead has high tyre life (slow, blocking) with small gap
    private boolean checkEmergenceTraffic(String driver, int settleLap) throws Exception {
        LapEvent driverLap = lapEvents.get(lapKey(settleLap, driver));
        if (driverLap == null) {
            return false;
        }

        int targetPosition = driverLap.getPosition() - 1;
        if (targetPosition < 1) {
            return false; // no car ahead if driver is in P1
        }

        Integer maxLap = globalMaxLap.value();
        if (maxLap == null) {
            maxLap = settleLap;
        }

        List<String> staleDrivers = new ArrayList<>();
        for (String d : driverLatestLap.keys()) {
            Integer latestLap = driverLatestLap.get(d);
            if (latestLap == null || maxLap - latestLap > RECENT_LAP_WINDOW) {
                staleDrivers.add(d);
                continue;
            }

            LapEvent candidate = lapEvents.get(lapKey(settleLap, d));
            if (candidate != null && candidate.getPosition() == targetPosition) {
                return candidate.getTyreLife() >= TRAFFIC_TYRE_LIFE_THRESHOLD
                        && driverLap.getGapToCarAhead() != null
                        && driverLap.getGapToCarAhead() < TRAFFIC_GAP_THRESHOLD_SECONDS;
            }
        }

        for (String stale : staleDrivers) {
            driverLatestLap.remove(stale);
        }
        return false;
    }

    // computes the directed time gap from driverA to driverB on a given lap.
    // positive = A is behind B, negative = A is ahead.
    // walks the position ladder, summing gapToCarAhead from each intermediate position.
    private Double computeDirectedGap(String driverA, String driverB, int lap) throws Exception {
        // direct O(1) lookups for driver positions
        LapEvent eventA = lapEvents.get(lapKey(lap, driverA));
        LapEvent eventB = lapEvents.get(lapKey(lap, driverB));

        if (eventA == null || eventB == null) {
            return null;
        }

        int posA = eventA.getPosition();
        int posB = eventB.getPosition();

        // pit-cycle evaluation compares net rivals, not the full field spread.
        // if drivers are too far apart in position, the inferred summed gap is noisy
        // and should not be treated as a strategic undercut/defend signal.
        if (Math.abs(posA - posB) > MAX_PLAUSIBLE_RIVAL_POSITION_DELTA) {
            return null;
        }

        if (posA == posB) {
            return 0.0;
        }

        // build position map only for the lap we need
        HashMap<Integer, LapEvent> byPosition = new HashMap<>();
        Integer maxLap = globalMaxLap.value();
        if (maxLap == null) {
            maxLap = lap;
        }

        List<String> staleDrivers = new ArrayList<>();
        for (String d : driverLatestLap.keys()) {
            Integer latestLap = driverLatestLap.get(d);
            if (latestLap == null || maxLap - latestLap > RECENT_LAP_WINDOW) {
                staleDrivers.add(d);
                continue;
            }

            LapEvent e = lapEvents.get(lapKey(lap, d));
            if (e != null) {
                byPosition.put(e.getPosition(), e);
            }
        }

        for (String stale : staleDrivers) {
            driverLatestLap.remove(stale);
        }

        int lo = Math.min(posA, posB);
        int hi = Math.max(posA, posB);
        double gap = 0.0;

        for (int pos = lo + 1; pos <= hi; pos++) {
            LapEvent car = byPosition.get(pos);
            if (car == null || car.getGapToCarAhead() == null) {
                return null;
            }
            gap += car.getGapToCarAhead();
        }

        return posA > posB ? gap : -gap;
    }

    // finds a driver at a specific position on a given lap.
    // falls back to lap-1 if the target position isn't in state yet, which happens
    // when cars cross the finish line sequentially (e.g. P3 hasn't arrived when P2 pits).
    // the grid from the previous lap is valid for identifying net rivals at pit entry.
    private RivalSnapshot findDriverAtPosition(int targetPos, int lap, String currentDriver) throws Exception {
        if (targetPos < 1) {
            return null;
        }

        Integer maxLap = globalMaxLap.value();
        if (maxLap == null) {
            maxLap = lap;
        }

        List<String> staleDrivers = new ArrayList<>();
        for (int searchLap = lap; searchLap >= Math.max(1, lap - 1); searchLap--) {
            for (String d : driverLatestLap.keys()) {
                Integer latestLap = driverLatestLap.get(d);
                if (latestLap == null || maxLap - latestLap > RECENT_LAP_WINDOW) {
                    staleDrivers.add(d);
                    continue;
                }

                LapEvent e = lapEvents.get(lapKey(searchLap, d));
                if (e != null && e.getPosition() == targetPos && !e.getDriver().equals(currentDriver)) {
                    for (String stale : staleDrivers) {
                        driverLatestLap.remove(stale);
                    }
                    return new RivalSnapshot(e.getDriver(), e.getStint(), searchLap);
                }
            }
        }

        for (String stale : staleDrivers) {
            driverLatestLap.remove(stale);
        }
        return null;
    }

    // updates stint best for a driver, tracking best green-flag lap per stint
    private void updateStintBest(LapEvent event) throws Exception {
        if (event.getLapTime() == null || event.getLapTime() <= 0) {
            return;
        }
        if (event.getPitInTime() != null || event.getPitOutTime() != null) {
            return;
        }
        if (!TrackStatusCodes.isGreenOrUnknown(event.getTrackStatus())) {
            return;
        }

        String key = stintKey(event.getDriver(), event.getStint());
        Double current = stintBests.get(key);
        if (current == null || event.getLapTime() < current) {
            stintBests.put(key, event.getLapTime());
        }
    }

    // fallback resolution when safety timer fires
    private void resolveFromTimer(PitCycle cycle, Collector<PitStopEvaluationAlert> out)
            throws Exception {
        if (!ensurePrimaryRivalContext(cycle)) {
            int settleOrDefault = cycle.getSettleLap() > 0
                ? cycle.getSettleLap()
                : cycle.getPitLap() + SETTLE_LAPS;
            int forcedCurrentLap = settleOrDefault + GAP_SEARCH_WINDOW;
            if (!tryResolveWithReferenceFallback(cycle, forcedCurrentLap, out)) {
                emitResult(cycle, null, null, Result.UNRESOLVED_MISSING_RIVAL,
                        RESOLUTION_INSUFFICIENT_DATA, false, out);
            }
            return;
        }

        double baselineLap = ensureValidBaseline(cycle, cycle.getPitLap());
        if (baselineLap <= 0) {
            emitResult(cycle, null, null, Result.UNRESOLVED_INVALID_BASELINE,
                    RESOLUTION_INSUFFICIENT_DATA, false, out);
            return;
        }

        int settleLap = cycle.getSettleLap();
        if (settleLap < 0) {
            settleLap = cycle.getPitLap() + SETTLE_LAPS;
        }

        Double postGap = null;
        for (int lap = settleLap + TIMER_FALLBACK_LOOKBACK; lap >= settleLap; lap--) {
            postGap = computeDirectedGap(cycle.getDriver(), cycle.getPrimaryRival(), lap);
            if (postGap != null) {
                break;
            }
        }

        if (postGap == null) {
            if (tryResolveWithPaceShiftFallback(cycle, settleLap,
                    baselineLap, RESOLUTION_SAFETY_TIMER_PACE_SHIFT, out)) {
                return;
            }
            // this reasoning is from garcia tejada 2023, final rescue via ordinal rival order before unresolved.
            if (tryResolveWithPositionalFallback(cycle, settleLap, true, out)) {
                return;
            }

            emitResult(cycle, null, null, Result.UNRESOLVED_MISSING_POST_GAP,
                    RESOLUTION_INSUFFICIENT_DATA, false, out);
            return;
        }

        Double gapDeltaPct = computeGapDeltaPct(cycle.getPrePitGapToPrimary(), postGap,
        baselineLap);
        boolean emergenceTraffic = checkEmergenceTraffic(cycle.getDriver(), settleLap);

        Result result = classifyPitStop(cycle.getPrePitGapToPrimary(), postGap,
        baselineLap, cycle.isDriverPittedFirst(),
                cycle.getTrackStatusAtPit(), cycle.getCompoundAfterPit(), emergenceTraffic);

        emitResult(cycle, postGap, gapDeltaPct, result, "SAFETY_TIMER", false, out);
    }

    private void emitResult(PitCycle cycle, Double postPitGap, Double gapDeltaPct,
            Result result, String resolvedVia, boolean isOffset,
            Collector<PitStopEvaluationAlert> out) {

        PitStopEvaluationAlert alert = PitStopEvaluationAlert.create(
                cycle.getDriver(), cycle.getPitLap(), result);
        alert.setRivalAhead(cycle.getRivalAhead());
        alert.setRivalBehind(cycle.getRivalBehind());
        alert.setPrePitGapAhead(cycle.getPrePitGapAhead());
        alert.setPrePitGapBehind(cycle.getPrePitGapBehind());
        alert.setPostPitGapToRival(postPitGap);
        alert.setCompound(cycle.getCompoundAfterPit());
        alert.setTrackStatusAtPit(cycle.getTrackStatusAtPit());
        alert.setTyreAgeAtPit(cycle.getTyreAgeAtPit());
        alert.setGapToCarAheadAtPit(cycle.getGapToCarAheadAtPit());
        alert.setRace(cycle.getRace());
        alert.setGapDeltaPct(gapDeltaPct);
        alert.setBaselineLapTime(cycle.getBaselineLapTime() > 0 ? cycle.getBaselineLapTime() : null);
        alert.setDriverPittedFirst(cycle.isDriverPittedFirst());
        alert.setOffsetStrategy(isOffset);
        alert.setResolvedVia(resolvedVia);

        out.collect(alert);
        LOG.info("pit eval: {} lap {} -> {} (rival: {}, delta: {}%, via: {})",
                cycle.getDriver(), cycle.getPitLap(), result,
                cycle.getPrimaryRival(),
                gapDeltaPct != null ? String.format("%.2f", gapDeltaPct) : "N/A",
                resolvedVia);
    }

    private void emitWarmupUnresolved(LapEvent event, Collector<PitStopEvaluationAlert> out) {
        PitCycle cycle = new PitCycle();
        cycle.setDriver(event.getDriver());
        cycle.setPitLap(event.getLapNumber());
        cycle.setTrackStatusAtPit(event.getTrackStatus());
        cycle.setTyreAgeAtPit(event.getTyreLife());
        cycle.setGapToCarAheadAtPit(event.getGapToCarAhead());
        cycle.setRace(event.getRace());
        cycle.setBaselineLapTime(0.0);

        emitResult(
                cycle,
                null,
                null,
            Result.UNRESOLVED_MISSING_PRE_GAP,
                RESOLUTION_EARLY_LAP_FILTER,
                false,
                out);
    }
}
