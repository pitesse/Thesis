package com.polimi.f1.operators.groundtruth;

import java.io.Serializable;
import java.util.ArrayList;
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

// classifies completed pit stops as success (undercut/defend) or failure (lost position)
// by comparing the pitting driver against their net rival (car directly ahead at pit time).
//
// keyed by constant "RACE" for global field visibility, same pattern as DropZoneEvaluator.
// the core insight: raw position always drops when pitting (~22s loss), so comparing
// pre/post position naively labels almost every pit stop as a failure. instead, we track
// the pitting driver's position relative to their direct rival after both have settled.
//
// ex: VER (P2) pits lap 20, drops to P5. rival LEC (P3 at pit time) pits lap 23, drops to P6.
//     after settling, VER P3 vs LEC P4 -> VER ahead -> SUCCESS_UNDERCUT.
//
// lifecycle:
//   1. detect pit entry (pitInTime != null), snapshot field to find the net rival (car at P-1)
//   2. wait for pitting driver to complete 3 post-pit laps (skip out-lap)
//   3. wait for rival to also pit and settle, or timeout after 10 laps if rival stays out
//   4. compare positions: A ahead of B -> UNDERCUT, same -> DEFEND, behind -> FAILURE
//   5. safety timer: 15 min event-time to clear stale records (retirements, race end)
public class PitStopEvaluator
        extends KeyedProcessFunction<String, LapEvent, PitStopEvaluationAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(PitStopEvaluator.class);

    // laps to wait after a pit stop before the position is considered "settled"
    private static final int SETTLE_LAPS = 3;

    // if the rival has not pitted within this many laps of the driver's pit, resolve anyway
    private static final int RIVAL_PATIENCE_LAPS = 10;

    // safety timeout: 15 minutes of event time to clear stale pending pits
    private static final long TIMER_TIMEOUT_MS = 900_000;

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
            if (rival != null) {
                rivalDriver = rival.getDriver();
            } else if (event.getPosition() == 1) {
                // P1 has no car ahead. use car directly behind as the comparison target.
                rivalDriver = findCarBehind(event);
            }

            PitRecord record = new PitRecord();
            record.driver = driver;
            record.pitLap = lap;
            record.prePitPosition = event.getPosition();
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

            LOG.info("Pit detected: {} on lap {} at P{}, rival: {}",
                    driver, lap, event.getPosition(), rivalDriver);
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
                resolveWithCurrentPositions(record, out);
                toRemove.add(entry.getKey());
            }
        }
        for (String key : toRemove) {
            pendingPits.remove(key);
        }
    }

    // attempts to resolve pending evaluations. for each pending pit record:
    // 1. check if the pitting driver has SETTLE_LAPS post-pit laps (excluding out-lap)
    // 2. check if the rival has also pitted and settled, or if RIVAL_PATIENCE_LAPS exceeded
    // 3. if resolvable, classify and emit
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

            if (record.netRival == null) {
                // no rival identified (isolated driver or edge case), compare raw positions
                int postPos = driverAtSettle.getPosition();
                Result result;
                if (postPos < record.prePitPosition) {
                    result = Result.SUCCESS_UNDERCUT;
                } else if (postPos == record.prePitPosition) {
                    result = Result.SUCCESS_DEFEND;
                } else {
                    result = Result.FAILURE_LOST_POSITION;
                }
                emit(record, postPos, result, out);
                resolved.add(entry.getKey());
                continue;
            }

            // check if rival has also pitted and settled
            PitRecord rivalPit = pendingPits.get(record.netRival);
            boolean rivalSettled = false;

            if (rivalPit != null) {
                int rivalSettleLap = rivalPit.pitLap + SETTLE_LAPS + 1;
                LapEvent rivalAtSettle = lapEvents.get(lapKey(rivalSettleLap, rivalPit.driver));
                if (rivalAtSettle != null && currentLap >= rivalSettleLap) {
                    rivalSettled = true;
                }
            }

            // if rival hasn't pitted, check patience threshold
            boolean patienceExpired = (currentLap >= record.pitLap + RIVAL_PATIENCE_LAPS);

            if (rivalSettled || patienceExpired) {
                int driverPos = findLatestPosition(record.driver, currentLap);
                int rivalPos = findLatestPosition(record.netRival, currentLap);

                if (driverPos <= 0 || rivalPos <= 0) {
                    if (patienceExpired) {
                        resolveWithCurrentPositions(record, out);
                        resolved.add(entry.getKey());
                    }
                    continue;
                }

                Result result = classify(record, driverPos, rivalPos);
                emit(record, driverPos, result, out);
                resolved.add(entry.getKey());
            }
        }

        for (String key : resolved) {
            pendingPits.remove(key);
        }
    }

    // classifies the pit stop outcome by comparing driver vs rival positions.
    // for P1 pitting (where rival is the car behind), success means staying ahead.
    // for others (rival is the car ahead), success means overtaking or matching.
    private Result classify(PitRecord record, int driverPos, int rivalPos) {
        if (record.prePitPosition == 1) {
            // P1 pitted, rival was P2 (car behind). staying ahead = defend.
            return driverPos <= rivalPos ? Result.SUCCESS_DEFEND : Result.FAILURE_LOST_POSITION;
        }
        // standard: rival was the car ahead. gaining position = undercut.
        if (driverPos < rivalPos) {
            return Result.SUCCESS_UNDERCUT;
        } else if (driverPos == rivalPos) {
            return Result.SUCCESS_DEFEND;
        } else {
            return Result.FAILURE_LOST_POSITION;
        }
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

    // walks backward from the given lap to find the most recent position for a driver
    private int findLatestPosition(String driver, int currentLap) throws Exception {
        for (int lap = currentLap; lap >= Math.max(1, currentLap - 10); lap--) {
            LapEvent event = lapEvents.get(lapKey(lap, driver));
            if (event != null) {
                return event.getPosition();
            }
        }
        return -1;
    }

    // fallback resolution when patience expired or timer fired
    private void resolveWithCurrentPositions(PitRecord record, Collector<PitStopEvaluationAlert> out) throws Exception {
        int searchLap = record.pitLap + RIVAL_PATIENCE_LAPS + SETTLE_LAPS;
        int driverPos = findLatestPosition(record.driver, searchLap);
        int postPos = driverPos > 0 ? driverPos : record.prePitPosition;

        Result result;
        if (record.netRival != null) {
            int rivalPos = findLatestPosition(record.netRival, searchLap);
            if (rivalPos > 0 && driverPos > 0) {
                result = classify(record, driverPos, rivalPos);
            } else {
                result = postPos <= record.prePitPosition ? Result.SUCCESS_DEFEND : Result.FAILURE_LOST_POSITION;
            }
        } else {
            result = postPos <= record.prePitPosition ? Result.SUCCESS_DEFEND : Result.FAILURE_LOST_POSITION;
        }

        emit(record, postPos, result, out);
    }

    private void emit(PitRecord record, int postPitPosition, Result result,
            Collector<PitStopEvaluationAlert> out) {
        out.collect(new PitStopEvaluationAlert(
                record.driver,
                record.pitLap,
                record.prePitPosition,
                postPitPosition,
                record.compound,
                result,
                record.trackStatus,
                record.tyreAge,
                record.gapToCarAhead,
                record.race,
                record.netRival
        ));
        LOG.info("Pit evaluation: {} lap {} -> {} (rival: {}, pre: P{}, post: P{})",
                record.driver, record.pitLap, result, record.netRival,
                record.prePitPosition, postPitPosition);
    }

    // serializable record of a pending pit stop awaiting resolution.
    // stored in MapState keyed by driver abbreviation.
    public static class PitRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        public String driver;
        public int pitLap;
        public int prePitPosition;
        public String netRival;
        public String compound;
        public String trackStatus;
        public int tyreAge;
        public Double gapToCarAhead;
        public String race;
        public long registeredTimestamp;
    }
}
