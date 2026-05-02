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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.model.TrackStatusCodes;
import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.output.DropZoneAlert;

// computes the physical race drop zone for each driver: the exact track position
// they would emerge at after a pit stop, given the position ladder and pit loss.
//
// in F1 strategy, the "net race" is the classification order (championship rivals).
// the "physical race" is who you end up behind on track after pitting, which determines
// clean air vs dirty air. this evaluator bridges both: net rival (car ahead in classification)
// and physical emergence position after pit loss.
//
// architecture: leader-driven state machine. all lap events flow into a single keyed
// partition (keyed by constant "RACE"). when the race leader (position == 1) completes
// lap N, lap N-1 is guaranteed complete for the entire surviving field. this triggers
// evaluation without any timers or watermark dependencies.
//
// this design bypasses a flink limitation: keying by lapNumber causes idle partitions
// to stall the local watermark indefinitely, preventing event-time timers from firing.
// keying on a constant with a race-physics trigger (leader crossing the line) is correct.
//
// state: flat MapState with composite keys "lapNumber:driver" (e.g. "15:VER").
// flink's pojo serializer silently fails with nested generic collections like
// MapState<Integer, List<LapEvent>>, producing empty lists on state access.
//
// algorithm: for each driver at position P with pit loss L, walk the position ladder
// from P+1 downward, summing gapToCarAhead. when cumulativeGap >= L, the driver emerges
// between the previous and current car. gap to physical car = L - cumulative up to that car.
// ex: VER at P2, pitLoss=22.0s. ladder: P3=3.0, P4=1.5, P5=2.0, P6=5.5, P7=15.0.
//     cumulative: 3.0, 4.5, 6.5, 12.0, 27.0. 27.0 >= 22.0 -> emerge behind P6, gap=10.0s.
//
// only evaluates drivers with tyre life >= 8 laps (filters opening-lap noise and
// immediate post-pit evaluations). suppresses evaluation under yellow/red flags.
public class DropZoneEvaluator
        extends KeyedProcessFunction<String, LapEvent, DropZoneAlert> {

    // minimum tire age (laps) before strategic pit stop evaluation is meaningful
    private static final int MIN_TYRE_LIFE = 8;
    private static final int RECENT_LAP_WINDOW = 3;
    private static final String STATUS_LOSS_ESTIMATED = "LOSS_ESTIMATED";
    private static final String STATUS_NO_LOSS_FEASIBLE = "NO_LOSS_FEASIBLE";
    private static final String STATUS_INELIGIBLE_TYRE_AGE = "INELIGIBLE_TYRE_AGE";
    private static final String STATUS_INELIGIBLE_TRACK_STATUS = "INELIGIBLE_TRACK_STATUS";
    private static final String STATUS_INSUFFICIENT_GAP_CONTEXT = "INSUFFICIENT_GAP_CONTEXT";

    // latest event per driver, used as a compact physical-grid snapshot
    private transient MapState<String, LapEvent> latestGridState;

    // max observed lap across all events, used as a stall-safe trigger
    private transient ValueState<Integer> maxLapState;

    @Override
    public void open(OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, LapEvent> gridDesc
                = new MapStateDescriptor<>("drop-zone-latest-grid", String.class, LapEvent.class);
        gridDesc.enableTimeToLive(ttlConfig);
        latestGridState = getRuntimeContext().getMapState(gridDesc);

        ValueStateDescriptor<Integer> maxLapDesc
                = new ValueStateDescriptor<>("drop-zone-max-lap", Integer.class);
        maxLapDesc.enableTimeToLive(ttlConfig);
        maxLapState = getRuntimeContext().getState(maxLapDesc);
    }

    // accumulates each incoming lap event and checks for the leader trigger.
    // when P1 finishes lap N, triggers evaluation of the previous lap's position ladder.
    @Override
    public void processElement(LapEvent event, Context ctx, Collector<DropZoneAlert> out) throws Exception {
        if (event == null || event.getDriver() == null || event.getLapNumber() <= 0) {
            return;
        }

        int lap = event.getLapNumber();
        latestGridState.put(event.getDriver(), event);

        Integer maxLap = maxLapState.value();
        if (maxLap == null) {
            maxLap = 0;
        }

        if (lap > maxLap) {
            maxLapState.update(lap);

            if (lap > 1) {
                List<LapEvent> currentGrid = new ArrayList<>();
                for (LapEvent e : latestGridState.values()) {
                    if (lap - e.getLapNumber() <= RECENT_LAP_WINDOW) {
                        currentGrid.add(e);
                    }
                }
                if (!currentGrid.isEmpty()) {
                    evaluate(currentGrid, out);
                }
            }
        }
    }

    // walks the position ladder for each eligible driver to compute where they
    // would physically emerge after a pit stop. sorts by position, then for each
    // driver with sufficient tyre life, accumulates gaps downward until the
    // cumulative gap exceeds the pit loss, identifying the emergence position.
    private void evaluate(List<LapEvent> laps, Collector<DropZoneAlert> out) {
        laps.sort(Comparator.comparingInt(LapEvent::getPosition));

        for (int i = 0; i < laps.size(); i++) {
            LapEvent current = laps.get(i);
            String netRival = (i > 0) ? laps.get(i - 1).getDriver() : null;

            // skip fresh tires and opening laps
            if (current.getTyreLife() < MIN_TYRE_LIFE) {
                emitStatus(
                        out,
                        current,
                        netRival,
                        null,
                        0.0,
                        current.getPosition(),
                        0,
                        STATUS_INELIGIBLE_TYRE_AGE,
                        safePitLoss(selectPitLoss(current), current.getPitLoss())
                );
                continue;
            }

            // select pit loss based on track status at this lap
            Double pitLoss = selectPitLoss(current);
            if (pitLoss == null) {
                emitStatus(
                        out,
                        current,
                        netRival,
                        null,
                        0.0,
                        current.getPosition(),
                        0,
                        STATUS_INELIGIBLE_TRACK_STATUS,
                        safePitLoss(current.getPitLoss(), 0.0)
                );
                continue;
            }

            // walk the position ladder to find emergence position.
            // cumulativeGap tracks the total time gap from the evaluating driver
            // to each successive car behind them.
            double cumulativeGap = 0;
            LapEvent physicalCarAhead = null;
            double gapToPhysicalCar = 0;
            boolean missingGapContext = false;
            boolean noLossFeasible = false;

            for (int j = i + 1; j < laps.size(); j++) {
                LapEvent behind = laps.get(j);
                Double gap = behind.getGapToCarAhead();
                if (gap == null) {
                    missingGapContext = true;
                    break;
                }

                cumulativeGap += gap;

                if (cumulativeGap >= pitLoss) {
                    // driver emerges between the car at j-1 and j.
                    // if j == i+1, the very first car behind already covers the pit loss,
                    // meaning gap behind > pitLoss -> no positions lost -> safe pit.
                    if (j == i + 1) {
                        noLossFeasible = true;
                        break;
                    }
                    physicalCarAhead = laps.get(j - 1);
                    // remaining gap to the physical car ahead after pit stop.
                    // ex: pitLoss=22.0, cumulative to j-1 was 12.0 -> 22.0 - 12.0 = 10.0s behind P6
                    gapToPhysicalCar = pitLoss - (cumulativeGap - gap);
                    break;
                }

                // haven't reached pit loss yet, this car is ahead of us after pit.
                // if we exhaust the ladder without exceeding pit loss, driver drops to last.
                physicalCarAhead = behind;
                gapToPhysicalCar = pitLoss - cumulativeGap;
            }

            if (missingGapContext && physicalCarAhead == null && !noLossFeasible) {
                emitStatus(
                        out,
                        current,
                        netRival,
                        null,
                        0.0,
                        current.getPosition(),
                        0,
                        STATUS_INSUFFICIENT_GAP_CONTEXT,
                        pitLoss
                );
                continue;
            }

            if (noLossFeasible || physicalCarAhead == null) {
                emitStatus(
                        out,
                        current,
                        netRival,
                        null,
                        0.0,
                        current.getPosition(),
                        0,
                        STATUS_NO_LOSS_FEASIBLE,
                        pitLoss
                );
                continue;
            }

            int emergencePosition = physicalCarAhead.getPosition() + 1;
            int positionsLost = emergencePosition - current.getPosition();
            if (positionsLost <= 0) {
                emitStatus(
                        out,
                        current,
                        netRival,
                        physicalCarAhead,
                        Math.max(0.0, gapToPhysicalCar),
                        current.getPosition(),
                        0,
                        STATUS_NO_LOSS_FEASIBLE,
                        pitLoss
                );
                continue;
            }

            emitStatus(
                    out,
                    current,
                    netRival,
                    physicalCarAhead,
                    Math.max(0.0, gapToPhysicalCar),
                    emergencePosition,
                    positionsLost,
                    STATUS_LOSS_ESTIMATED,
                    pitLoss
            );
        }
    }

    private static void emitStatus(
            Collector<DropZoneAlert> out,
            LapEvent current,
            String netRival,
            LapEvent physicalCarAhead,
            double gapToPhysicalCar,
            int emergencePosition,
            int positionsLost,
            String dropZoneStatus,
            double pitLoss) {
        out.collect(new DropZoneAlert(
                current.getRace(),
                current.getDriver(),
                current.getLapNumber(),
                current.getPosition(),
                emergencePosition,
                positionsLost,
                netRival,
                physicalCarAhead != null ? physicalCarAhead.getDriver() : null,
                gapToPhysicalCar,
                physicalCarAhead != null ? physicalCarAhead.getCompound() : null,
                physicalCarAhead != null ? physicalCarAhead.getTyreLife() : -1,
                dropZoneStatus,
                TrackStatusCodes.normalizeOrGreen(current.getTrackStatus()),
                pitLoss
        ));
    }

    private static double safePitLoss(Double preferred, Double fallback) {
        if (preferred != null) {
            return preferred;
        }
        if (fallback != null) {
            return fallback;
        }
        return 0.0;
    }

    // selects the appropriate pit loss value based on FIA track status code.
    // each lap event carries three pit loss values (green, VSC, SC) from the producer.
    // returns null for yellow/red flags to suppress evaluation (pit lane may be closed).
    private static Double selectPitLoss(LapEvent lap) {
        String status = lap.getTrackStatus();
        status = TrackStatusCodes.normalizeOrGreen(status);

        return switch (status) {
            case TrackStatusCodes.GREEN ->
                lap.getPitLoss();
            case TrackStatusCodes.VIRTUAL_SAFETY_CAR, TrackStatusCodes.VSC_ENDING ->
                lap.getVscPitLoss();
            case TrackStatusCodes.SAFETY_CAR ->
                lap.getScPitLoss();
            // yellow ("2"), red ("5"): suppress, pit lane may be closed
            default ->
                null;
        };
    }
}
