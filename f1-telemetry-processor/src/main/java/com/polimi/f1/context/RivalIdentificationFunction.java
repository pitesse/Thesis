package com.polimi.f1.context;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.polimi.f1.events.LapEvent;
import com.polimi.f1.model.RivalInfoAlert;

// collects all lap events in a session-gap window (keyed by lap number) and identifies
// rivals for each driver by sorting on position.
// for each driver, emits a RivalInfoAlert with the driver ahead/behind and their respective gaps.
//
// session window with 30s gap: all drivers' lap events for the same lap arrive within seconds
// of each other (Python producer interleaves chronologically). 30s is generous enough to
// handle jitter without merging across laps.
public class RivalIdentificationFunction
        extends ProcessWindowFunction<LapEvent, RivalInfoAlert, Integer, TimeWindow> {

    @Override
    public void process(
            Integer lapNumber,
            ProcessWindowFunction<LapEvent, RivalInfoAlert, Integer, TimeWindow>.Context context,
            Iterable<LapEvent> elements,
            Collector<RivalInfoAlert> out) {

        // collect all lap events in this window and sort by position
        List<LapEvent> laps = new ArrayList<>();
        elements.forEach(laps::add);
        laps.sort(Comparator.comparingInt(LapEvent::getPosition));

        for (int i = 0; i < laps.size(); i++) {
            LapEvent current = laps.get(i);
            String driverAhead = null;
            String driverBehind = null;
            Double gapAhead = null;
            Double gapBehind = null;

            // leave null if no driver ahead (we're P1) )
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
                            current.getScPitLoss()
                    ));
        }
    }
}
