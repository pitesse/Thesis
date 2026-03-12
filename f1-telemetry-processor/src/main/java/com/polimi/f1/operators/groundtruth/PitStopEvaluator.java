package com.polimi.f1.operators.groundtruth;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.output.PitStopEvaluationAlert;
import com.polimi.f1.model.output.PitStopEvaluationAlert.Result;

// classifies completed pit cycles using a state machine that tracks rival clusters,
// detects offset strategies (1-stop vs 2-stop), and normalizes gap deltas as percentages
// of the driver's baseline lap time for track-agnostic evaluation.
//
// keyed by constant "RACE" for global field visibility.
//
// state machine:
//   PENDING_SETTLE   -> driver just pitted, waiting for out-lap + 3 settle laps
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
// 8 labels: SUCCESS_UNDERCUT, SUCCESS_OVERCUT, SUCCESS_DEFEND, SUCCESS_FREE_STOP,
//           OFFSET_ADVANTAGE, OFFSET_DISADVANTAGE, FAILURE_PACE_DEFICIT, FAILURE_TRAFFIC
public class PitStopEvaluator
        extends KeyedProcessFunction<String, LapEvent, PitStopEvaluationAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(PitStopEvaluator.class);

    // laps to wait after pit for gap to settle (excludes out-lap)
    private static final int SETTLE_LAPS = 3;

    // safety timeout: 15 minutes event time to clear stale records
    private static final long TIMER_TIMEOUT_MS = 900_000;

    // percentage thresholds for gap classification (as % of baseline lap time)
    // ex: 0.5% of 80s baseline = 0.4s, of 105s baseline = 0.525s
    private static final double UNDERCUT_THRESHOLD_PCT = 0.5;
    private static final double DEFEND_BAND_PCT = 0.5;

    // free stop threshold: pit under sc/vsc with gap change < 1% of baseline
    private static final double FREE_STOP_THRESHOLD_PCT = 1.0;

    // fraction of remaining race distance before declaring offset strategy
    private static final double OFFSET_RACE_FRACTION = 0.4;

    // traffic detection: car ahead with tyreLife >= this is considered slow/passable,
    // but car ahead within this gap is considered blocking
    private static final int TRAFFIC_TYRE_LIFE_THRESHOLD = 25;

    // flat state: all lap events, key = "lapNumber:driver"
    private transient MapState<String, LapEvent> lapEvents;

    // pending pit cycles, keyed by driver abbreviation
    private transient MapState<String, PitCycle> pendingCycles;

    // stint best per driver per stint, key = "driver:stint"
    private transient MapState<String, Double> stintBests;

    @Override
    public void open(Configuration parameters) {
        // 2h ttl: prevents unbounded state growth over continuous streaming.
        // race data older than 2 hours is no longer relevant for pit cycle resolution.
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2))
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
        int lap = event.getLapNumber();
        String driver = event.getDriver();

        lapEvents.put(lapKey(lap, driver), event);
        updateStintBest(event);

        // detect pit entry: pitInTime != null means this driver entered the pits this lap
        if (event.getPitInTime() != null && !pendingCycles.contains(driver)) {
            registerPitCycle(event, ctx);
        }

        // try to advance any pending cycles on every incoming event
        advanceCycles(event, ctx, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PitStopEvaluationAlert> out)
            throws Exception {
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, PitCycle> entry : pendingCycles.entries()) {
            PitCycle cycle = entry.getValue();
            if (cycle.safetyTimerTimestamp <= timestamp) {
                LOG.info("safety timer: resolving {} pit on lap {}", cycle.driver, cycle.pitLap);
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
        cycle.driver = driver;
        cycle.pitLap = lap;
        cycle.stintBeforePit = event.getStint();
        cycle.trackStatusAtPit = event.getTrackStatus();
        cycle.tyreAgeAtPit = event.getTyreLife();
        cycle.gapToCarAheadAtPit = event.getGapToCarAhead();
        cycle.race = event.getRace();
        cycle.totalLaps = event.getTotalLaps();
        cycle.settleLap = lap + SETTLE_LAPS + 1;
        cycle.state = CycleState.PENDING_SETTLE;

        // capture baseline: stint best from the pre-pit stint
        String sKey = stintKey(driver, event.getStint());
        Double best = stintBests.get(sKey);
        cycle.baselineLapTime = (best != null) ? best : 0.0;

        // rival cluster: car ahead and car behind.
        // findDriverAtPosition falls back to lap-1 when sequential arrival
        // means the target position hasn't been stored yet for the current lap.
        RivalSnapshot ahead = findDriverAtPosition(position - 1, lap);
        RivalSnapshot behind = findDriverAtPosition(position + 1, lap);

        if (ahead != null) {
            cycle.rivalAhead = ahead.driver;
            cycle.rivalAheadStintAtPit = ahead.stint;
            // compute gap on the lap where the rival was actually found
            cycle.prePitGapAhead = computeDirectedGap(driver, ahead.driver, ahead.foundOnLap);
            if (cycle.prePitGapAhead == null) {
                cycle.prePitGapAhead = computeDirectedGap(driver, ahead.driver, lap);
            }
        }
        if (behind != null) {
            cycle.rivalBehind = behind.driver;
            cycle.rivalBehindStintAtPit = behind.stint;
            cycle.prePitGapBehind = computeDirectedGap(driver, behind.driver, behind.foundOnLap);
            if (cycle.prePitGapBehind == null) {
                cycle.prePitGapBehind = computeDirectedGap(driver, behind.driver, lap);
            }
        }

        // determine primary rival: car ahead for most drivers, car behind for P1
        if (position == 1 && behind != null) {
            cycle.primaryRival = behind.driver;
            cycle.prePitGapToPrimary = cycle.prePitGapBehind;
            cycle.primaryRivalStintAtPit = behind.stint;
        } else if (ahead != null) {
            cycle.primaryRival = ahead.driver;
            cycle.prePitGapToPrimary = cycle.prePitGapAhead;
            cycle.primaryRivalStintAtPit = ahead.stint;
        }

        // offset timeout: 40% of remaining race distance
        int remainingLaps = Math.max(1, cycle.totalLaps - lap);
        cycle.offsetTimeoutLap = lap + (int) Math.ceil(remainingLaps * OFFSET_RACE_FRACTION);

        // safety timer
        cycle.safetyTimerTimestamp = event.getEventTimeMillis() + TIMER_TIMEOUT_MS;
        ctx.timerService().registerEventTimeTimer(cycle.safetyTimerTimestamp);

        // the compound after pit is unknown until the next lap's event arrives.
        // will be filled in during settle phase.
        cycle.compoundAfterPit = null;

        pendingCycles.put(driver, cycle);

        LOG.info("pit cycle registered: {} lap {} P{}, rivals: [ahead={}, behind={}], "
                + "baseline: {}, offset timeout lap: {}",
                driver, lap, position, cycle.rivalAhead, cycle.rivalBehind,
                cycle.baselineLapTime > 0 ? String.format("%.3f", cycle.baselineLapTime) : "N/A",
                cycle.offsetTimeoutLap);
    }

    // advances all pending cycles based on incoming events
    private void advanceCycles(LapEvent trigger, Context ctx, Collector<PitStopEvaluationAlert> out)
            throws Exception {
        int currentLap = trigger.getLapNumber();
        List<String> resolved = new ArrayList<>();

        for (Map.Entry<String, PitCycle> entry : pendingCycles.entries()) {
            PitCycle cycle = entry.getValue();

            // fill compound after pit from first post-pit lap
            if (cycle.compoundAfterPit == null
                    && trigger.getDriver().equals(cycle.driver)
                    && trigger.getLapNumber() > cycle.pitLap) {
                cycle.compoundAfterPit = trigger.getCompound();
                pendingCycles.put(entry.getKey(), cycle);
            }

            switch (cycle.state) {
                case PENDING_SETTLE:
                    if (currentLap >= cycle.settleLap) {
                        LapEvent driverAtSettle = lapEvents.get(lapKey(cycle.settleLap, cycle.driver));
                        if (driverAtSettle != null) {
                            cycle.state = CycleState.PENDING_RIVAL;
                            pendingCycles.put(entry.getKey(), cycle);
                            if (tryResolveRival(cycle, currentLap, out)) {
                                resolved.add(entry.getKey());
                            }
                        }
                    }
                    break;

                case PENDING_RIVAL:
                    if (tryResolveRival(cycle, currentLap, out)) {
                        resolved.add(entry.getKey());
                    }
                    break;

                default:
                    break;
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

        if (cycle.primaryRival == null || cycle.prePitGapToPrimary == null) {
            emitResult(cycle, null, null, Result.SUCCESS_DEFEND, "RIVAL_PIT", false, out);
            return true;
        }

        // check if primary rival has pitted: either after our pit (undercut scenario)
        // or shortly before our pit (overcut scenario, rival already on new tires)
        boolean rivalPittedAfter = hasRivalPitted(cycle.primaryRival, cycle.primaryRivalStintAtPit,
                cycle.pitLap, currentLap);

        // overcut check: rival pitted in the window before our pit (within SETTLE_LAPS + 2 laps)
        int lookbackStart = Math.max(1, cycle.pitLap - SETTLE_LAPS - 2);
        boolean rivalPittedBefore = hasRivalPittedInRange(cycle.primaryRival,
                lookbackStart, cycle.pitLap);

        boolean rivalPitted = rivalPittedAfter || rivalPittedBefore;

        if (rivalPitted) {
            int rivalPitLap;
            if (rivalPittedBefore) {
                rivalPitLap = findRivalPitLapInRange(cycle.primaryRival, lookbackStart, cycle.pitLap);
            } else {
                rivalPitLap = findRivalPitLap(cycle.primaryRival, cycle.primaryRivalStintAtPit,
                        cycle.pitLap, currentLap);
            }
            cycle.driverPittedFirst = (rivalPitLap < 0 || cycle.pitLap <= rivalPitLap);

            // wait for rival to also settle before comparing gaps
            int rivalSettleLap = (rivalPitLap > 0) ? (rivalPitLap + SETTLE_LAPS + 1) : cycle.settleLap;
            int comparisonLap = Math.max(cycle.settleLap, rivalSettleLap);

            Double postGap = findGapNearLap(cycle.driver, cycle.primaryRival, comparisonLap);
            if (postGap == null && currentLap >= comparisonLap + 2) {
                postGap = findGapNearLap(cycle.driver, cycle.primaryRival, cycle.settleLap);
            }
            if (postGap == null && currentLap < comparisonLap + 3) {
                return false; // wait for more data
            }

            boolean emergenceTraffic = checkEmergenceTraffic(cycle.driver, cycle.settleLap);

            Result result = classifyPitStop(cycle.prePitGapToPrimary, postGap,
                    cycle.baselineLapTime, cycle.driverPittedFirst,
                    cycle.trackStatusAtPit, emergenceTraffic, false);

            Double gapDeltaPct = computeGapDeltaPct(cycle.prePitGapToPrimary, postGap,
                    cycle.baselineLapTime);

            emitResult(cycle, postGap, gapDeltaPct, result, "RIVAL_PIT", false, out);
            return true;
        }

        // check offset timeout: rival hasn't pitted after 40% of remaining race
        if (currentLap >= cycle.offsetTimeoutLap) {
            Double currentGap = findGapNearLap(cycle.driver, cycle.primaryRival, currentLap);
            if (currentGap == null) {
                currentGap = findGapNearLap(cycle.driver, cycle.primaryRival, currentLap - 1);
            }

            Double gapDeltaPct = computeGapDeltaPct(cycle.prePitGapToPrimary, currentGap,
                    cycle.baselineLapTime);
            Result result;
            if (gapDeltaPct != null && gapDeltaPct < -DEFEND_BAND_PCT) {
                result = Result.OFFSET_ADVANTAGE;
            } else if (gapDeltaPct != null && gapDeltaPct > DEFEND_BAND_PCT) {
                result = Result.OFFSET_DISADVANTAGE;
            } else {
                result = Result.OFFSET_ADVANTAGE;
            }

            emitResult(cycle, currentGap, gapDeltaPct, result, "OFFSET_TIMEOUT", true, out);
            return true;
        }

        return false;
    }

    // 8-label classification based on gap delta percentage
    private Result classifyPitStop(Double prePitGap, Double postPitGap,
            double baselineLap, boolean driverPittedFirst,
            String trackStatus, boolean emergenceTraffic, boolean isOffset) {

        if (prePitGap == null || postPitGap == null || baselineLap <= 0) {
            return Result.SUCCESS_DEFEND;
        }

        Double gapDeltaPct = computeGapDeltaPct(prePitGap, postPitGap, baselineLap);
        if (gapDeltaPct == null) {
            return Result.SUCCESS_DEFEND;
        }

        // sc/vsc free stop: pitted under caution with minimal gap distortion
        if (trackStatus != null && (trackStatus.equals("4") || trackStatus.equals("6")
                || trackStatus.equals("7"))) {
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

    // computes gap change as % of baseline lap time.
    // ex: prePitGap=2.5, postPitGap=1.0, baseline=82.0 -> (1.0-2.5)/82.0*100 = -1.83%
    private static Double computeGapDeltaPct(Double prePitGap, Double postPitGap, double baseline) {
        if (prePitGap == null || postPitGap == null || baseline <= 0) {
            return null;
        }
        return (postPitGap - prePitGap) / baseline * 100.0;
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

    // searches for gap data near the target lap, scanning +-3 laps for missing data
    private Double findGapNearLap(String driverA, String driverB, int targetLap) throws Exception {
        Double gap = computeDirectedGap(driverA, driverB, targetLap);
        if (gap != null) {
            return gap;
        }
        for (int offset = 1; offset <= 3; offset++) {
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

        String prefix = settleLap + ":";
        for (Map.Entry<String, LapEvent> entry : lapEvents.entries()) {
            if (entry.getKey().startsWith(prefix)) {
                LapEvent candidate = entry.getValue();
                if (candidate.getPosition() == driverLap.getPosition() - 1) {
                    return candidate.getTyreLife() >= TRAFFIC_TYRE_LIFE_THRESHOLD
                            && driverLap.getGapToCarAhead() != null
                            && driverLap.getGapToCarAhead() < 2.0;
                }
            }
        }
        return false;
    }

    // computes the directed time gap from driverA to driverB on a given lap.
    // positive = A is behind B, negative = A is ahead.
    // walks the position ladder, summing gapToCarAhead from each intermediate position.
    private Double computeDirectedGap(String driverA, String driverB, int lap) throws Exception {
        HashMap<Integer, LapEvent> byPosition = new HashMap<>();
        String prefix = lap + ":";
        int posA = -1, posB = -1;

        for (Map.Entry<String, LapEvent> entry : lapEvents.entries()) {
            if (entry.getKey().startsWith(prefix)) {
                LapEvent e = entry.getValue();
                byPosition.put(e.getPosition(), e);
                if (e.getDriver().equals(driverA)) {
                    posA = e.getPosition();
                }
                if (e.getDriver().equals(driverB)) {
                    posB = e.getPosition();
                }
            }
        }

        if (posA < 0 || posB < 0) {
            return null;
        }
        if (posA == posB) {
            return 0.0;
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
    private RivalSnapshot findDriverAtPosition(int targetPos, int lap) throws Exception {
        if (targetPos < 1) {
            return null;
        }
        for (int searchLap = lap; searchLap >= Math.max(1, lap - 1); searchLap--) {
            String prefix = searchLap + ":";
            for (Map.Entry<String, LapEvent> entry : lapEvents.entries()) {
                if (entry.getKey().startsWith(prefix)) {
                    LapEvent e = entry.getValue();
                    if (e.getPosition() == targetPos) {
                        return new RivalSnapshot(e.getDriver(), e.getStint(), searchLap);
                    }
                }
            }
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
        if (event.getTrackStatus() != null && !event.getTrackStatus().equals("1")) {
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
        if (cycle.primaryRival == null || cycle.prePitGapToPrimary == null) {
            emitResult(cycle, null, null, Result.SUCCESS_DEFEND, "SAFETY_TIMER", false, out);
            return;
        }

        Double postGap = null;
        for (int lap = cycle.settleLap + 5; lap >= cycle.settleLap; lap--) {
            postGap = computeDirectedGap(cycle.driver, cycle.primaryRival, lap);
            if (postGap != null) {
                break;
            }
        }

        if (postGap == null) {
            emitResult(cycle, null, null, Result.SUCCESS_DEFEND, "SAFETY_TIMER", false, out);
            return;
        }

        Double gapDeltaPct = computeGapDeltaPct(cycle.prePitGapToPrimary, postGap,
                cycle.baselineLapTime);
        boolean emergenceTraffic = checkEmergenceTraffic(cycle.driver, cycle.settleLap);

        Result result = classifyPitStop(cycle.prePitGapToPrimary, postGap,
                cycle.baselineLapTime, cycle.driverPittedFirst,
                cycle.trackStatusAtPit, emergenceTraffic, false);

        emitResult(cycle, postGap, gapDeltaPct, result, "SAFETY_TIMER", false, out);
    }

    private void emitResult(PitCycle cycle, Double postPitGap, Double gapDeltaPct,
            Result result, String resolvedVia, boolean isOffset,
            Collector<PitStopEvaluationAlert> out) {

        PitStopEvaluationAlert alert = PitStopEvaluationAlert.create(
                cycle.driver, cycle.pitLap, result);
        alert.setRivalAhead(cycle.rivalAhead);
        alert.setRivalBehind(cycle.rivalBehind);
        alert.setPrePitGapAhead(cycle.prePitGapAhead);
        alert.setPrePitGapBehind(cycle.prePitGapBehind);
        alert.setPostPitGapToRival(postPitGap);
        alert.setCompound(cycle.compoundAfterPit);
        alert.setTrackStatusAtPit(cycle.trackStatusAtPit);
        alert.setTyreAgeAtPit(cycle.tyreAgeAtPit);
        alert.setGapToCarAheadAtPit(cycle.gapToCarAheadAtPit);
        alert.setRace(cycle.race);
        alert.setGapDeltaPct(gapDeltaPct);
        alert.setBaselineLapTime(cycle.baselineLapTime > 0 ? cycle.baselineLapTime : null);
        alert.setDriverPittedFirst(cycle.driverPittedFirst);
        alert.setOffsetStrategy(isOffset);
        alert.setResolvedVia(resolvedVia);

        out.collect(alert);
        LOG.info("pit eval: {} lap {} -> {} (rival: {}, delta: {}%, via: {})",
                cycle.driver, cycle.pitLap, result,
                cycle.primaryRival,
                gapDeltaPct != null ? String.format("%.2f", gapDeltaPct) : "N/A",
                resolvedVia);
    }

    // snapshot of a rival at pit detection time, including which lap the data came from
    private static class RivalSnapshot {

        final String driver;
        final int stint;
        final int foundOnLap;

        RivalSnapshot(String driver, int stint, int foundOnLap) {
            this.driver = driver;
            this.stint = stint;
            this.foundOnLap = foundOnLap;
        }
    }

    // state for each pending pit cycle
    public static class PitCycle implements Serializable {

        private static final long serialVersionUID = 3L;

        public String driver;
        public int pitLap;
        public int stintBeforePit;
        public String compoundAfterPit;
        public String trackStatusAtPit;
        public int tyreAgeAtPit;
        public double baselineLapTime;
        public int totalLaps;

        // rival cluster
        public String rivalAhead;
        public String rivalBehind;
        public Double prePitGapAhead;
        public Double prePitGapBehind;
        public int rivalAheadStintAtPit;
        public int rivalBehindStintAtPit;

        // primary rival (ahead for most, behind for P1)
        public String primaryRival;
        public Double prePitGapToPrimary;
        public int primaryRivalStintAtPit;
        public boolean driverPittedFirst;

        // resolution
        public CycleState state;
        public int settleLap;
        public int offsetTimeoutLap;
        public long safetyTimerTimestamp;
        public Double gapToCarAheadAtPit;
        public String race;
    }

    public enum CycleState {
        PENDING_SETTLE,
        PENDING_RIVAL,
    }
}
