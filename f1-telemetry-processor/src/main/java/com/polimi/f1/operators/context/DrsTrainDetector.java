package com.polimi.f1.operators.context;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.polimi.f1.model.output.RivalInfoAlert;

// detects DRS trains: contiguous groups of drivers where consecutive cars are
// within 1 second of each other, making DRS-assisted overtaking ineffective.
//
// operates on RivalInfoAlert output from RivalIdentificationFunction, re-windowed
// by lap number with a 10s session gap. sorts rivals by position and scans for
// contiguous groups where gapAhead < 1.0s. a group of 2+ cars is a DRS train.
//
// strategically significant: a driver stuck in a train cannot effectively use DRS
// to overtake because the car ahead also has DRS from the car in front of it.
// pit stop timing or alternative strategy becomes the only realistic overtaking tool.
public class DrsTrainDetector
        extends ProcessWindowFunction<RivalInfoAlert, String, Integer, TimeWindow> {

    private static final double DRS_THRESHOLD_SECONDS = 1.0;

    @Override
    public void process(
            Integer lapNumber,
            ProcessWindowFunction<RivalInfoAlert, String, Integer, TimeWindow>.Context context,
            Iterable<RivalInfoAlert> elements,
            Collector<String> out) {

        List<RivalInfoAlert> rivals = new ArrayList<>();
        elements.forEach(rivals::add);
        rivals.sort(Comparator.comparing(RivalInfoAlert::getPosition));

        if (rivals.size() < 2) {
            return;
        }

        // identify contiguous groups where gap to car ahead < 1s.
        // a group of 2+ drivers with consecutive gaps < 1s forms a drs train.
        List<List<RivalInfoAlert>> trains = new ArrayList<>();
        List<RivalInfoAlert> currentTrain = new ArrayList<>();
        currentTrain.add(rivals.get(0));

        for (int i = 1; i < rivals.size(); i++) {
            RivalInfoAlert current = rivals.get(i);
            Double gap = current.getGapAhead();
            if (gap != null && gap < DRS_THRESHOLD_SECONDS) {
                currentTrain.add(current);
            } else {
                if (currentTrain.size() >= 2) {
                    trains.add(currentTrain);
                }
                currentTrain = new ArrayList<>();
                currentTrain.add(current);
            }
        }
        if (currentTrain.size() >= 2) {
            trains.add(currentTrain);
        }

        // emit alert for each driver in each detected train
        for (List<RivalInfoAlert> train : trains) {
            StringBuilder drivers = new StringBuilder();
            for (RivalInfoAlert r : train) {
                if (drivers.length() > 0) drivers.append(", ");
                drivers.append(r.getDriver()).append(" (P").append(r.getPosition()).append(")");
            }
            String trainInfo = String.format("DRS TRAIN | Lap: %d | %d cars: [%s]",
                    lapNumber, train.size(), drivers);
            out.collect(trainInfo);
        }
    }
}
