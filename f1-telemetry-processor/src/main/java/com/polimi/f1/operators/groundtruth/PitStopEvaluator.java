package com.polimi.f1.operators.groundtruth;

import java.time.Duration;
import java.util.ArrayList;
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
// 8 labels: SUCCESS_UNDERCUT, SUCCESS_OVERCUT, SUCCESS_DEFEND, SUCCESS_FREE_STOP,
//           OFFSET_ADVANTAGE, OFFSET_DISADVANTAGE, FAILURE_PACE_DEFICIT, FAILURE_TRAFFIC
public class PitStopEvaluator
        extends KeyedProcessFunction<String, LapEvent, PitStopEvaluationAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(PitStopEvaluator.class);

    // required number of completed green laps after pit before evaluation
    private static final int SETTLE_LAPS = 4;

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
            registerPitCycle(event, ctx);
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

        // capture baseline: stint best from the pre-pit stint
        String sKey = stintKey(driver, event.getStint());
        Double best = stintBests.get(sKey);
        cycle.setBaselineLapTime((best != null) ? best : 0.0);

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
                    if (status == null || status.equals("1")) {
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

        if (cycle.getPrimaryRival() == null || cycle.getPrePitGapToPrimary() == null) {
            emitResult(cycle, null, null, Result.SUCCESS_DEFEND, "RIVAL_PIT", false, out);
            return true;
        }

        // check if primary rival has pitted: either after our pit (undercut scenario)
        // or shortly before our pit (overcut scenario, rival already on new tires)
        boolean rivalPittedAfter = hasRivalPitted(cycle.getPrimaryRival(), cycle.getPrimaryRivalStintAtPit(),
                cycle.getPitLap(), currentLap);

        // overcut check: rival pitted in the window before our pit (within SETTLE_LAPS + 2 laps)
        int lookbackStart = Math.max(1, cycle.getPitLap() - SETTLE_LAPS - 2);
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
            if (postGap == null && currentLap >= comparisonLap + 2) {
                postGap = findGapNearLap(cycle.getDriver(), cycle.getPrimaryRival(), cycle.getSettleLap());
            }
            if (postGap == null && currentLap < comparisonLap + 3) {
                return false; // wait for more data
            }

            boolean emergenceTraffic = checkEmergenceTraffic(cycle.getDriver(), cycle.getSettleLap());

            Result result = classifyPitStop(cycle.getPrePitGapToPrimary(), postGap,
                    cycle.getBaselineLapTime(), cycle.isDriverPittedFirst(),
                    cycle.getTrackStatusAtPit(), emergenceTraffic);

            Double gapDeltaPct = computeGapDeltaPct(cycle.getPrePitGapToPrimary(), postGap,
                    cycle.getBaselineLapTime());

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
                    cycle.getBaselineLapTime());
            Result result;
            if (gapDeltaPct != null && gapDeltaPct < -DEFEND_BAND_PCT) {
                result = Result.OFFSET_ADVANTAGE;
            } else if (gapDeltaPct != null && gapDeltaPct > DEFEND_BAND_PCT) {
                result = Result.OFFSET_DISADVANTAGE;
            } else {
                // neutral or missing data: default to defensive success to avoid false positive bias
                result = Result.SUCCESS_DEFEND;
            }

            emitResult(cycle, currentGap, gapDeltaPct, result, "OFFSET_TIMEOUT", true, out);
            return true;
        }

        return false;
    }

    // 8-label classification based on gap delta percentage
    private Result classifyPitStop(Double prePitGap, Double postPitGap,
            double baselineLap, boolean driverPittedFirst,
            String trackStatus, boolean emergenceTraffic) {

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

    // calculates the rival settle lap as the lap where rival reaches settle_laps green laps after pit
    private int calculateRivalSettleLap(String rival, int rivalPitLap, int currentLap) throws Exception {
        if (rivalPitLap < 0) {
            return -1;
        }

        int greenLaps = 0;
        for (int l = rivalPitLap + 1; l <= currentLap; l++) {
            LapEvent e = lapEvents.get(lapKey(l, rival));
            if (e != null && ("1".equals(e.getTrackStatus()) || e.getTrackStatus() == null)) {
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
            if (latestLap == null || maxLap - latestLap > 3) {
                staleDrivers.add(d);
                continue;
            }

            LapEvent candidate = lapEvents.get(lapKey(settleLap, d));
            if (candidate != null && candidate.getPosition() == targetPosition) {
                return candidate.getTyreLife() >= TRAFFIC_TYRE_LIFE_THRESHOLD
                        && driverLap.getGapToCarAhead() != null
                        && driverLap.getGapToCarAhead() < 2.0;
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
            if (latestLap == null || maxLap - latestLap > 3) {
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
                if (latestLap == null || maxLap - latestLap > 3) {
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
        if (cycle.getPrimaryRival() == null || cycle.getPrePitGapToPrimary() == null) {
            emitResult(cycle, null, null, Result.SUCCESS_DEFEND, "SAFETY_TIMER", false, out);
            return;
        }

        int settleLap = cycle.getSettleLap();
        if (settleLap < 0) {
            settleLap = cycle.getPitLap() + SETTLE_LAPS;
        }

        Double postGap = null;
        for (int lap = settleLap + 5; lap >= settleLap; lap--) {
            postGap = computeDirectedGap(cycle.getDriver(), cycle.getPrimaryRival(), lap);
            if (postGap != null) {
                break;
            }
        }

        if (postGap == null) {
            emitResult(cycle, null, null, Result.SUCCESS_DEFEND, "SAFETY_TIMER", false, out);
            return;
        }

        Double gapDeltaPct = computeGapDeltaPct(cycle.getPrePitGapToPrimary(), postGap,
                cycle.getBaselineLapTime());
        boolean emergenceTraffic = checkEmergenceTraffic(cycle.getDriver(), settleLap);

        Result result = classifyPitStop(cycle.getPrePitGapToPrimary(), postGap,
                cycle.getBaselineLapTime(), cycle.isDriverPittedFirst(),
                cycle.getTrackStatusAtPit(), emergenceTraffic);

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
}
