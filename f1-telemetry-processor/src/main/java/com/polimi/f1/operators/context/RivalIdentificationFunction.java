package com.polimi.f1.operators.context;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.output.MLFeatureRow;
import com.polimi.f1.model.output.RivalInfoAlert;

// identifies the car ahead and behind each driver on every lap by collecting
// all lap events in a session-gap window (keyed by lap number) and sorting by position.
//
// emits a RivalInfoAlert per driver with the identified rival ahead/behind and their
// time gaps. also emits an MLFeatureRow side output for each driver with valid lap time,
// combining positional gap context with timing and tire data for downstream ML training.
//
// session window with 30s gap: all drivers' lap events for the same lap arrive within
// seconds of each other (python producer interleaves chronologically). 30s handles jitter
// without merging events from consecutive laps (~80s apart at 1x speed).
public class RivalIdentificationFunction
    extends ProcessWindowFunction<LapEvent, RivalInfoAlert, String, TimeWindow> {

    // side output tag for denormalized ML feature rows, collected via getSideOutput() in the main job
    public static final OutputTag<MLFeatureRow> ML_FEATURES_TAG =
            new OutputTag<MLFeatureRow>("ml-features") {};

    @Override
    public void process(
            String raceLapKey,
            ProcessWindowFunction<LapEvent, RivalInfoAlert, String, TimeWindow>.Context context,
            Iterable<LapEvent> lapEvents,
            Collector<RivalInfoAlert> out) {

        // collect all lap events in this window and sort by position
        List<LapEvent> laps = new ArrayList<>();
        lapEvents.forEach(laps::add);
        if (laps.isEmpty()) {
            return;
        }
        laps.sort(Comparator.comparingInt(LapEvent::getPosition));

        int lapNumber = laps.get(0).getLapNumber();

        for (int i = 0; i < laps.size(); i++) {
            LapEvent current = laps.get(i);
            String driverAhead = null;
            String driverBehind = null;
            Double gapAhead = null;
            Double gapBehind = null;

            // leave null if no driver ahead (we're P1)
            if (i > 0) {
                LapEvent ahead = laps.get(i - 1);
                driverAhead = ahead.getDriver();
                gapAhead = current.getGapToCarAhead();
            }

            // leave null if no driver behind (we're last)
            if (i < laps.size() - 1) {
                LapEvent behind = laps.get(i + 1);
                driverBehind = behind.getDriver();
                // gap behind = the next car's gap-to-car-ahead (which is us)
                gapBehind = behind.getGapToCarAhead();
            }

            out.collect(
                    new RivalInfoAlert(
                            current.getDriver(),
                            driverAhead,
                            driverBehind,
                            gapAhead,
                            gapBehind,
                            lapNumber,
                            current.getPosition(),
                            current.getRace(),
                            current.getPitLoss(),
                            current.getVscPitLoss(),
                            current.getScPitLoss(),
                            current.getTyreLife(),
                            current.getCompound()
                    ));

            // ml side output: denormalized feature row combining lap data with gap context.
            // only emit for laps with valid timing (null lapTime = pit in/out laps).
            if (current.getLapTime() != null) {
                context.output(ML_FEATURES_TAG, new MLFeatureRow(
                        current.getRace(),
                        current.getDriver(),
                        lapNumber,
                        current.getPosition(),
                        current.getCompound(),
                        current.getTyreLife(),
                        current.getTrackTemp(),
                        current.getAirTemp(),
                        current.getHumidity(),
                        current.getRainfall(),
                        current.getSpeedTrap(),
                        current.getTeam(),
                        gapAhead,
                        gapBehind,
                        current.getLapTime(),
                        current.getPitLoss(),
                        current.getTrackStatus()
                ));
            }
        }
    }
}
