package com.polimi.f1.operators.realtime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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

    // flat state accumulating lap events across the entire field.
    // key format: "lapNumber:driver", e.g. "15:VER"
    private transient MapState<String, LapEvent> lapEvents;

    @Override
    public void open(OpenContext openContext) {
        lapEvents = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("lap-events", String.class, LapEvent.class));
    }

    private static String stateKey(int lap, String driver) {
        return lap + ":" + driver;
    }

    // accumulates each incoming lap event and checks for the leader trigger.
    // when P1 finishes lap N, triggers evaluation of the previous lap's position ladder.
    @Override
    public void processElement(LapEvent event, Context ctx, Collector<DropZoneAlert> out) throws Exception {
        int lap = event.getLapNumber();
        lapEvents.put(stateKey(lap, event.getDriver()), event);

        // leader-driven trigger: when P1 finishes lap N, evaluate lap N-1
        if (event.getPosition() == 1) {
            if (lap > 1) {
                int previousLap = lap - 1;
                List<LapEvent> previousLapEvents = collectLap(previousLap);
                if (!previousLapEvents.isEmpty()) {
                    evaluate(previousLapEvents, out);
                    removeLap(previousLap, previousLapEvents);
                }
            }
        }
    }

    // collects all events for a given lap number by scanning the flat map for matching prefix
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

    // walks the position ladder for each eligible driver to compute where they
    // would physically emerge after a pit stop. sorts by position, then for each
    // driver with sufficient tyre life, accumulates gaps downward until the
    // cumulative gap exceeds the pit loss, identifying the emergence position.
    private void evaluate(List<LapEvent> laps, Collector<DropZoneAlert> out) {
        laps.sort(Comparator.comparingInt(LapEvent::getPosition));

        for (int i = 0; i < laps.size(); i++) {
            LapEvent current = laps.get(i);

            // skip fresh tires and opening laps
            if (current.getTyreLife() < MIN_TYRE_LIFE) {
                continue;
            }

            // select pit loss based on track status at this lap
            Double pitLoss = selectPitLoss(current);
            if (pitLoss == null) {
                continue;
            }

            // walk the position ladder to find emergence position.
            // cumulativeGap tracks the total time gap from the evaluating driver
            // to each successive car behind them.
            double cumulativeGap = 0;
            LapEvent physicalCarAhead = null;
            double gapToPhysicalCar = 0;

            for (int j = i + 1; j < laps.size(); j++) {
                LapEvent behind = laps.get(j);
                Double gap = behind.getGapToCarAhead();
                if (gap == null) {
                    break;
                }

                cumulativeGap += gap;

                if (cumulativeGap >= pitLoss) {
                    // driver emerges between the car at j-1 and j.
                    // if j == i+1, the very first car behind already covers the pit loss,
                    // meaning gap behind > pitLoss -> no positions lost -> safe pit.
                    if (j == i + 1) {
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

            // only emit if driver would actually lose positions
            if (physicalCarAhead != null) {
                int emergencePosition = physicalCarAhead.getPosition() + 1;
                int positionsLost = emergencePosition - current.getPosition();

                // net rival: car immediately ahead in the classification (the undercut target)
                String netRival = (i > 0) ? laps.get(i - 1).getDriver() : null;

                String status = current.getTrackStatus() != null ? current.getTrackStatus() : "1";

                out.collect(new DropZoneAlert(
                        current.getDriver(),
                        current.getLapNumber(),
                        current.getPosition(),
                        emergencePosition,
                        positionsLost,
                        netRival,
                        physicalCarAhead.getDriver(),
                        gapToPhysicalCar,
                        physicalCarAhead.getCompound(),
                        physicalCarAhead.getTyreLife(),
                        status,
                        pitLoss
                ));
            }
        }
    }

    // selects the appropriate pit loss value based on FIA track status code.
    // each lap event carries three pit loss values (green, VSC, SC) from the producer.
    // returns null for yellow/red flags to suppress evaluation (pit lane may be closed).
    private static Double selectPitLoss(LapEvent lap) {
        String status = lap.getTrackStatus();
        if (status == null) {
            status = "1";
        }

        return switch (status) {
            case "1" ->
                lap.getPitLoss();
            case "6", "7" ->
                lap.getVscPitLoss();
            case "4" ->
                lap.getScPitLoss();
            // yellow ("2"), red ("5"): suppress, pit lane may be closed
            default ->
                null;
        };
    }
}
