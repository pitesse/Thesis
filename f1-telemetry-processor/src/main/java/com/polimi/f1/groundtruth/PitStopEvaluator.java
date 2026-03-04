package com.polimi.f1.groundtruth;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.events.LapEvent;
import com.polimi.f1.model.PitStopEvaluationAlert;
import com.polimi.f1.model.PitStopEvaluationAlert.Result;

// evaluates pit stop outcomes by comparing position before and after the stop.
// keyed by driver, tracks state across laps:
//   1. detects pit entry (LapEvent with pitInTime != null)
//   2. records pre-pit position
//   3. collects the next 3 clean laps after the pit stop
//   4. classifies the outcome based on position delta
//
// uses event-time timers as a safety net to clear stale state if post-pit laps
// never arrive (e.g., driver retired after pit stop).
public class PitStopEvaluator extends KeyedProcessFunction<String, LapEvent, PitStopEvaluationAlert> {

    private static final int POST_PIT_LAPS = 3;
    // timeout: if 3 post-pit laps haven't arrived within 10 minutes, clear state (retirement)
    private static final long TIMER_TIMEOUT_MS = 10 * 60 * 1000;

    private transient ValueState<Boolean> hasPitted;
    private transient ValueState<Integer> pitLapNumber;
    private transient ValueState<Integer> prePitPosition;
    private transient ValueState<String> postPitCompound;
    private transient ListState<LapEvent> postPitLaps;

    @Override
    public void open(Configuration parameters) {
        hasPitted = getRuntimeContext().getState(
                new ValueStateDescriptor<>("has-pitted", Types.BOOLEAN));
        pitLapNumber = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pit-lap-number", Types.INT));
        prePitPosition = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pre-pit-position", Types.INT));
        postPitCompound = getRuntimeContext().getState(
                new ValueStateDescriptor<>("post-pit-compound", Types.STRING));
        postPitLaps = getRuntimeContext().getListState(
                new ListStateDescriptor<>("post-pit-laps", LapEvent.class));
    }

    @Override
    public void processElement(LapEvent lap, Context ctx, Collector<PitStopEvaluationAlert> out) throws Exception {
        Boolean pitted = hasPitted.value();

        // detect pit entry: record pre-pit position and start collecting post-pit laps
        if (lap.getPitInTime() != null && (pitted == null || !pitted)) {
            hasPitted.update(true);
            pitLapNumber.update(lap.getLapNumber());
            prePitPosition.update(lap.getPosition());
            postPitLaps.clear();
            postPitCompound.update(null);

            // safety timer: clear state if post-pit laps never arrive
            long timerTarget = lap.getEventTimeMillis() + TIMER_TIMEOUT_MS;
            ctx.timerService().registerEventTimeTimer(timerTarget);
            return;
        }

        // if we're collecting post-pit laps
        if (pitted != null && pitted) {
            // skip the out-lap (has pitOutTime set, lap times are unrepresentative)
            if (lap.getPitOutTime() != null) {
                // record the new compound from the out-lap
                if (lap.getCompound() != null) {
                    postPitCompound.update(lap.getCompound());
                }
                return;
            }

            // record compound if not yet captured (sometimes out-lap data is missing)
            if (postPitCompound.value() == null && lap.getCompound() != null) {
                postPitCompound.update(lap.getCompound());
            }

            postPitLaps.add(lap);

            List<LapEvent> collected = new ArrayList<>();
            postPitLaps.get().forEach(collected::add);

            if (collected.size() >= POST_PIT_LAPS) {
                // evaluate: use the position from the last collected lap as the post-pit position
                /*
                    TODO    some tracks have finish line half the main straight, so driver may get overtaken while in the pit because they are slower,
                            this doesn't represent real track position. maybe use the position a lap before (in that case need to check if an overtake 
                            happened before pitting but it's rare)
 
                */
                LapEvent lastLap = collected.get(collected.size() - 1);
                int prePos = prePitPosition.value();
                int postPos = lastLap.getPosition();

                Result result;
                if (postPos < prePos) {
                    result = Result.SUCCESS_UNDERCUT;
                } else if (postPos == prePos) {
                    result = Result.SUCCESS_DEFEND;
                } else {
                    result = Result.FAILURE_LOST_POSITION;
                }

                String compound = postPitCompound.value();
                if (compound == null) {
                    compound = "UNKNOWN";
                }

                out.collect(new PitStopEvaluationAlert(
                        lap.getDriver(),
                        pitLapNumber.value(),
                        prePos,
                        postPos,
                        compound,
                        result
                ));

                clearState();
            }
        }
    }

    // safety net: clears state if timer fires before enough post-pit laps arrive
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PitStopEvaluationAlert> out) throws Exception {
        Boolean pitted = hasPitted.value();
        if (pitted != null && pitted) {
            clearState();
        }
    }

    private void clearState() throws Exception {
        hasPitted.clear();
        pitLapNumber.clear();
        prePitPosition.clear();
        postPitCompound.clear();
        postPitLaps.clear();
    }
}
