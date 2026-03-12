package com.polimi.f1.operators.groundtruth;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.output.PitStopEvaluationAlert;
import com.polimi.f1.model.output.PitStopEvaluationAlert.Result;

// classifies completed pit stops as success (undercut/defend) or failure (lost time)
// by comparing the time gap between the pitting driver and their net rival before
// and after the pit stop settles.
//
// keyed by constant "RACE" for global field visibility, same pattern as DropZoneEvaluator.
// the core insight: raw position always drops when pitting (~22s loss), so comparing
// pre/post position naively labels almost every pit stop as a failure. instead, we measure
// the directed time gap between the driver and their net rival at pit time, then again
// 3 laps after the driver settles on new tires.
//
// ex: VER (P2, 2.5s behind LEC) pits lap 20. after settling on lap 24, VER is 1.0s behind LEC.
//     gap improved by 1.5s (> 0.5s tolerance) -> SUCCESS_UNDERCUT.
//
// gap computation: walks the position ladder at a given lap, summing gapToCarAhead
// from each intermediate position to compute the time delta between any two drivers.
//
// lifecycle:
//   1. detect pit entry (pitInTime != null), snapshot the field to find the net rival
//   2. compute directed gap between pitting driver and rival at pit time
//   3. wait for pitting driver to complete 3 post-pit laps (skip out-lap)
//   4. compute directed gap again, compare: improved -> UNDERCUT, maintained -> DEFEND, worsened -> FAILURE
//   5. safety timer: 15 min event-time to clear stale records (retirements, race end)
public class PitStopEvaluator
        extends KeyedProcessFunction<String, LapEvent, PitStopEvaluationAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(PitStopEvaluator.class);

    // laps to wait after a pit stop before the gap is considered "settled"
    private static final int SETTLE_LAPS = 3;

    // safety timeout: 15 minutes of event time to clear stale pending pits
    private static final long TIMER_TIMEOUT_MS = 900_000;

    // tolerance band for gap comparison (seconds). absorbs timing noise from
    // out-laps, track evolution, and measurement imprecision.
    private static final double GAP_TOLERANCE = 0.5;

    // flat state: all lap events, key = "lapNumber:driver"
    private transient MapState<String, LapEvent> lapEvents;

    // pending evaluations: one per driver who has pitted but not yet been resolved
    private transient MapState<String, PitRecord> pendingPits;

    @Override
    public void open(Configuration parameters) {
        lapEvents = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("pit-eval-laps", String.class, LapEvent.class));
        pendingPits = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("pending-pits", String.class, PitRecord.class));
    }

    private static String lapKey(int lap, String driver) {
        return lap + ":" + driver;
    }

    @Override
    public void processElement(LapEvent event, Context ctx, Collector<PitStopEvaluationAlert> out) throws Exception {
        int lap = event.getLapNumber();
        String driver = event.getDriver();

        lapEvents.put(lapKey(lap, driver), event);

        // detect pit entry: pitInTime != null means this driver entered the pits this lap
        if (event.getPitInTime() != null && !pendingPits.contains(driver)) {
            // snapshot the field from the most recent complete lap to find the net rival
            LapEvent rival = findNetRival(event);
            String rivalDriver = null;
            boolean wasLeader = false;
            if (rival != null) {
                rivalDriver = rival.getDriver();
            } else if (event.getPosition() == 1) {
                // P1 has no car ahead. use car directly behind as the comparison target.
                rivalDriver = findCarBehind(event);
                wasLeader = true;
            }

            // compute directed gap between pitting driver and rival at pit time
            Double prePitGap = null;
            if (rivalDriver != null) {
                prePitGap = computeDirectedGap(driver, rivalDriver, lap);
            }

            PitRecord record = new PitRecord();
            record.driver = driver;
            record.pitLap = lap;
            record.prePitGapToRival = prePitGap;
            record.wasLeader = wasLeader;
            record.netRival = rivalDriver;
            record.compound = event.getCompound();
            record.trackStatus = event.getTrackStatus();
            record.tyreAge = event.getTyreLife();
            record.gapToCarAhead = event.getGapToCarAhead();
            record.race = event.getRace();
            record.registeredTimestamp = event.getEventTimeMillis();

            pendingPits.put(driver, record);

            // safety timer: resolve or discard after 15 minutes of event time
            ctx.timerService().registerEventTimeTimer(event.getEventTimeMillis() + TIMER_TIMEOUT_MS);

            LOG.info("Pit detected: {} on lap {} at P{}, rival: {}, gap: {}",
                    driver, lap, event.getPosition(), rivalDriver,
                    prePitGap != null ? String.format("%.3f", prePitGap) : "N/A");
        }

        // try to resolve any pending evaluations on every incoming lap event
        tryResolve(event, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PitStopEvaluationAlert> out) throws Exception {
        // safety timeout: resolve any stale pending pits with whatever data we have
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, PitRecord> entry : pendingPits.entries()) {
            PitRecord record = entry.getValue();
            if (record.registeredTimestamp + TIMER_TIMEOUT_MS <= timestamp) {
                LOG.info("Timeout: resolving {} pit on lap {} via timer", record.driver, record.pitLap);
                resolveFromTimer(record, out);
                toRemove.add(entry.getKey());
            }
        }
        for (String key : toRemove) {
            pendingPits.remove(key);
        }
    }

    // resolves pending evaluations. for each pending pit record:
    // 1. check if the pitting driver has SETTLE_LAPS post-pit laps (excluding out-lap)
    // 2. compute the post-settle gap between driver and rival
    // 3. classify by comparing pre-pit gap vs post-settle gap
    private void tryResolve(LapEvent trigger, Collector<PitStopEvaluationAlert> out) throws Exception {
        int currentLap = trigger.getLapNumber();
        List<String> resolved = new ArrayList<>();

        for (Map.Entry<String, PitRecord> entry : pendingPits.entries()) {
            PitRecord record = entry.getValue();

            // driver must have settled: SETTLE_LAPS after pit + 1 for out-lap
            int driverSettleLap = record.pitLap + SETTLE_LAPS + 1;
            if (currentLap < driverSettleLap) {
                continue;
            }

            // check if pitting driver actually has data at the settle lap
            LapEvent driverAtSettle = lapEvents.get(lapKey(driverSettleLap, record.driver));
            if (driverAtSettle == null) {
                continue;
            }

            if (record.netRival == null || record.prePitGapToRival == null) {
                // no rival or no gap data (isolated driver or field edge), safe default
                emit(record, null, Result.SUCCESS_DEFEND, out);
                resolved.add(entry.getKey());
                continue;
            }

            // compute post-settle gap between driver and rival
            Double postGap = computeDirectedGap(record.driver, record.netRival, driverSettleLap);
            if (postGap == null) {
                // rival may have retired or data missing at exact settle lap, try adjacent laps
                for (int fallback = driverSettleLap - 1; fallback <= driverSettleLap + 2; fallback++) {
                    if (fallback == driverSettleLap) continue;
                    postGap = computeDirectedGap(record.driver, record.netRival, fallback);
                    if (postGap != null) break;
                }
            }
            if (postGap == null) {
                // still missing, resolve with unknown (defend as safe default for ml)
                emit(record, null, Result.SUCCESS_DEFEND, out);
                resolved.add(entry.getKey());
                continue;
            }

            Result result = classifyByGap(record.prePitGapToRival, postGap, record.wasLeader);
            emit(record, postGap, result, out);
            resolved.add(entry.getKey());
        }

        for (String key : resolved) {
            pendingPits.remove(key);
        }
    }

    // classifies pit stop outcome by comparing pre-pit gap vs post-settle gap.
    // for P1 (wasLeader=true, rival is car behind): success = still ahead (postGap <= 0).
    // for others (rival is car ahead): gap improved (delta < -tolerance) = undercut,
    // roughly maintained (|delta| <= tolerance) = defend, worsened (delta > tolerance) = failure.
    // the tolerance band absorbs timing noise from out-laps and track evolution.
    private static Result classifyByGap(double prePitGap, double postPitGap, boolean wasLeader) {
        if (wasLeader) {
            // leader pitting: rival is car behind. success = still ahead after settling.
            return postPitGap <= 0 ? Result.SUCCESS_DEFEND : Result.FAILURE_LOST_POSITION;
        }
        double delta = postPitGap - prePitGap;
        if (delta < -GAP_TOLERANCE) {
            return Result.SUCCESS_UNDERCUT;
        } else if (delta <= GAP_TOLERANCE) {
            return Result.SUCCESS_DEFEND;
        } else {
            return Result.FAILURE_LOST_POSITION;
        }
    }

    // computes the directed time gap from driverA to driverB on a given lap.
    // positive = A is behind B (B has lower position number), negative = A is ahead.
    // walks the position ladder, summing gapToCarAhead from each intermediate position.
    // ex: A at P5, B at P3 -> sum gapToCarAhead[P4] + gapToCarAhead[P5] = total gap A behind B
    // returns null if any intermediate gap data is missing (incomplete lap data)
    private Double computeDirectedGap(String driverA, String driverB, int lap) throws Exception {
        // collect all events for this lap into a position-indexed map
        HashMap<Integer, LapEvent> byPosition = new HashMap<>();
        String prefix = lap + ":";
        int posA = -1, posB = -1;

        for (Map.Entry<String, LapEvent> entry : lapEvents.entries()) {
            if (entry.getKey().startsWith(prefix)) {
                LapEvent e = entry.getValue();
                byPosition.put(e.getPosition(), e);
                if (e.getDriver().equals(driverA)) posA = e.getPosition();
                if (e.getDriver().equals(driverB)) posB = e.getPosition();
            }
        }

        if (posA < 0 || posB < 0) {
            return null;
        }
        if (posA == posB) {
            return 0.0;
        }

        // accumulate gaps from the higher position number toward the lower
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

        // positive = A behind B, negative = A ahead of B
        return posA > posB ? gap : -gap;
    }

    // finds the net rival: the car directly ahead of the pitting driver at pit time.
    // scans the current lap's events for the driver at position = pitDriver.position - 1.
    private LapEvent findNetRival(LapEvent pitDriver) throws Exception {
        if (pitDriver.getPosition() <= 1) {
            return null;
        }
        int targetPos = pitDriver.getPosition() - 1;
        int lap = pitDriver.getLapNumber();
        String prefix = lap + ":";

        for (Map.Entry<String, LapEvent> entry : lapEvents.entries()) {
            if (entry.getKey().startsWith(prefix)) {
                if (entry.getValue().getPosition() == targetPos) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    // finds the car directly behind the pitting driver (used when P1 pits)
    private String findCarBehind(LapEvent pitDriver) throws Exception {
        int targetPos = pitDriver.getPosition() + 1;
        int lap = pitDriver.getLapNumber();
        String prefix = lap + ":";

        for (Map.Entry<String, LapEvent> entry : lapEvents.entries()) {
            if (entry.getKey().startsWith(prefix)) {
                if (entry.getValue().getPosition() == targetPos) {
                    return entry.getValue().getDriver();
                }
            }
        }
        return null;
    }

    // fallback resolution when safety timer fires (driver retired, race ended, etc.)
    private void resolveFromTimer(PitRecord record, Collector<PitStopEvaluationAlert> out) throws Exception {
        if (record.netRival == null || record.prePitGapToRival == null) {
            emit(record, null, Result.SUCCESS_DEFEND, out);
            return;
        }

        // attempt to compute gap at the latest available lap near the settle window
        int searchLap = record.pitLap + SETTLE_LAPS + 1;
        Double postGap = null;
        for (int lap = searchLap + 5; lap >= searchLap; lap--) {
            postGap = computeDirectedGap(record.driver, record.netRival, lap);
            if (postGap != null) break;
        }

        if (postGap == null) {
            emit(record, null, Result.SUCCESS_DEFEND, out);
            return;
        }

        Result result = classifyByGap(record.prePitGapToRival, postGap, record.wasLeader);
        emit(record, postGap, result, out);
    }

    private void emit(PitRecord record, Double postPitGapToRival, Result result,
            Collector<PitStopEvaluationAlert> out) {
        out.collect(new PitStopEvaluationAlert(
                record.driver,
                record.pitLap,
                record.prePitGapToRival,
                postPitGapToRival,
                record.compound,
                result,
                record.trackStatus,
                record.tyreAge,
                record.gapToCarAhead,
                record.race,
                record.netRival
        ));
        LOG.info("Pit evaluation: {} lap {} -> {} (rival: {}, preGap: {}, postGap: {})",
                record.driver, record.pitLap, result, record.netRival,
                record.prePitGapToRival != null ? String.format("%.3f", record.prePitGapToRival) : "N/A",
                postPitGapToRival != null ? String.format("%.3f", postPitGapToRival) : "N/A");
    }

    // serializable record of a pending pit stop awaiting resolution.
    // stored in MapState keyed by driver abbreviation.
    public static class PitRecord implements Serializable {
        private static final long serialVersionUID = 2L;

        public String driver;
        public int pitLap;
        public Double prePitGapToRival;   // directed gap at pit time
        public boolean wasLeader;         // true if pitting from P1 (rival is car behind)
        public String netRival;
        public String compound;
        public String trackStatus;
        public int tyreAge;
        public Double gapToCarAhead;
        public String race;
        public long registeredTimestamp;
    }
}
