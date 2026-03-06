package com.polimi.f1.alerts;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.polimi.f1.events.TelemetryEvent;
import com.polimi.f1.model.LiftCoastAlert;

// detects lift & coast fuel-saving maneuvers using flink cep.
// pattern: throttle=100 -> throttle=0 -> brake>0, all within 2 seconds.
// followedBy (not next) allows non-relevant events between steps, real telemetry
// has intermediate throttle values and gear changes between pedal transitions.
// skipPastLastEvent prevents combinatorial explosion: at 4Hz, multiple consecutive
// full-throttle samples would each independently match the same lift+brake event.
//
// deduplication is required because cep with relaxed contiguity still fires N matches
// for N consecutive throttle=100 samples converging on the same lift+brake pair.
// the dedup function (LiftCoastDedup) suppresses duplicates by comparing the brake
// timestamp against the last emitted alert for each driver.
public class LiftCoastDetector {

    private LiftCoastDetector() {
    }

    // encapsulates the full lift & coast pipeline: pattern definition, cep matching,
    // and deduplication. expects an enriched telemetry stream (with track status set).
    public static DataStream<LiftCoastAlert> detect(DataStream<TelemetryEvent> enrichedTelemetry) {
        Pattern<TelemetryEvent, ?> liftAndCoastPattern = Pattern
                .<TelemetryEvent>begin("full-throttle", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<TelemetryEvent>() {
                    @Override
                    public boolean filter(TelemetryEvent e) {
                        return e.getThrottle() == 100;
                    }
                })
                .followedBy("lift")
                .where(new SimpleCondition<TelemetryEvent>() {
                    @Override
                    public boolean filter(TelemetryEvent e) {
                        return e.getThrottle() == 0;
                    }
                })
                .followedBy("coast-brake")
                .where(new SimpleCondition<TelemetryEvent>() {
                    @Override
                    public boolean filter(TelemetryEvent e) {
                        return e.getBrake() > 0;
                    }
                })
                .within(Time.seconds(2));

        PatternStream<TelemetryEvent> patternStream = CEP.pattern(
                enrichedTelemetry.keyBy(TelemetryEvent::getDriver),
                liftAndCoastPattern
        );

        DataStream<LiftCoastAlert> rawAlerts = patternStream
                .select((Map<String, List<TelemetryEvent>> match) -> {
                    TelemetryEvent ft = match.get("full-throttle").get(0);
                    TelemetryEvent li = match.get("lift").get(0);
                    TelemetryEvent br = match.get("coast-brake").get(0);
                    return new LiftCoastAlert(
                            ft.getDriver(), ft.getDate(), li.getDate(),
                            br.getDate(), ft.getTrackStatus());
                });

        return rawAlerts
                .keyBy(LiftCoastAlert::getDriver)
                .process(new LiftCoastDedup())
                .name("Lift & Coast Dedup");
    }

    // suppresses duplicate lift & coast alerts from the same braking zone.
    // cep with followedBy fires N matches for N consecutive throttle=100 samples that all
    // converge on the same lift+brake event. this function keeps only the first alert per
    // braking zone by comparing brake timestamps, any alert with the same brakeDate as the
    // previous one is a duplicate and gets dropped.
    static class LiftCoastDedup extends KeyedProcessFunction<String, LiftCoastAlert, LiftCoastAlert> {

        private transient ValueState<String> lastBrakeDate;

        @Override
        public void open(Configuration params) {
            lastBrakeDate = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastBrake", String.class)
            );
        }

        @Override
        public void processElement(LiftCoastAlert alert, Context ctx,
                Collector<LiftCoastAlert> out) throws Exception {
            String currentBrake = alert.getBrakeDate();
            String last = lastBrakeDate.value();
            if (!currentBrake.equals(last)) {
                out.collect(alert);
                lastBrakeDate.update(currentBrake);
            }
        }
    }
}
