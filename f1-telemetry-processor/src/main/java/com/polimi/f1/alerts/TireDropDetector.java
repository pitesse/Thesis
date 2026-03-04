package com.polimi.f1.alerts;

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
import com.polimi.f1.model.TireDropAlert;

// detects tire performance degradation by comparing a rolling 3-lap average
// to the best lap time in the current stint. alerts when the delta exceeds 1.5s.
//
// state is keyed by driver. on each stint change (new compound/pit stop), all state
// resets so a fresh baseline is established for the new tire set.
//
// pit in/out laps are excluded from the calculation since they are not representative
// of actual tire performance (pit lane speed, cold tires on out-lap).
public class TireDropDetector extends KeyedProcessFunction<String, LapEvent, TireDropAlert> {

    private static final double THRESHOLD_SECONDS = 1.5;
    private static final int ROLLING_WINDOW = 3; // number of laps to average for the rolling performance baseline

    // current stint number, used to detect stint changes (pit stop -> new tires)
    private transient ValueState<Integer> currentStint;
    // best clean lap time in the current stint (seconds)
    private transient ValueState<Double> stintBestLap;
    // circular buffer of the last N clean lap times (seconds)
    private transient ListState<Double> recentLapTimes;

    @Override
    public void open(Configuration parameters) {
        currentStint = getRuntimeContext().getState(
                new ValueStateDescriptor<>("current-stint", Types.INT));
        stintBestLap = getRuntimeContext().getState(
                new ValueStateDescriptor<>("stint-best-lap", Types.DOUBLE));
        recentLapTimes = getRuntimeContext().getListState(
                new ListStateDescriptor<>("recent-lap-times", Types.DOUBLE));
    }

    @Override
    public void processElement(LapEvent lap, Context ctx, Collector<TireDropAlert> out) throws Exception {
        // detect stint change: reset all state for new tire set
        Integer prevStint = currentStint.value();
        if (prevStint == null || prevStint != lap.getStint()) {
            currentStint.update(lap.getStint());
            stintBestLap.update(Double.MAX_VALUE);
            recentLapTimes.clear();
        }

        // skip pit in-laps and out-laps, they distort the rolling average
        if (lap.getPitInTime() != null || lap.getPitOutTime() != null) {
            return;
        }

        // skip laps with missing lap time (can happen with data quality issues)
        Double lapTimeSec = lap.getLapTime();
        if (lapTimeSec == null || lapTimeSec <= 0) {
            return;
        }

        // update stint best
        double best = stintBestLap.value();
        if (lapTimeSec < best) {
            stintBestLap.update(lapTimeSec);
            best = lapTimeSec;
        }

        // add to rolling buffer, keep only the last ROLLING_WINDOW entries
        List<Double> recent = new ArrayList<>();
        recentLapTimes.get().forEach(recent::add);
        recent.add(lapTimeSec);
        if (recent.size() > ROLLING_WINDOW) {
            recent = recent.subList(recent.size() - ROLLING_WINDOW, recent.size());
        }
        recentLapTimes.update(recent);

        // only evaluate once we have a full window
        if (recent.size() < ROLLING_WINDOW) {
            return;
        }

        double avg = recent.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double delta = avg - best;

        if (delta > THRESHOLD_SECONDS) {
            out.collect(new TireDropAlert(
                    lap.getDriver(),
                    lap.getLapNumber(),
                    lap.getCompound(),
                    lap.getTyreLife(),
                    avg,
                    best,
                    delta
            ));
        }
    }
}
