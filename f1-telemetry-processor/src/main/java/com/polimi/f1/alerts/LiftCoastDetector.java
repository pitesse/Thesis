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
// pattern: throttle=100 & speed>280 & gear>=7 -> throttle=0 -> brake>0, within 4 seconds.
//
// the speed and gear conditions on the "full-throttle" step filter out normal corner
// braking (where throttle also goes 100->0->brake). real lift & coast only happens on
// long straights at high speed in top gear, the extended 4s window allows for the
// coast phase which can last 1-3s before the driver applies brakes.
//
// followedBy (not next) allows non-relevant events between steps, real telemetry
// has intermediate throttle values and gear changes between pedal transitions.
// skipPastLastEvent prevents combinatorial explosion: at 4Hz, multiple consecutive
// full-throttle samples would each independently match the same lift+brake event.
//
// deduplication is required because cep with relaxed contiguity still fires N matches
// for N consecutive matching full-throttle samples converging on the same lift+brake pair.
// the dedup function suppresses duplicates by comparing the brake timestamp.
public class LiftCoastDetector {

    // minimum speed (km/h) to qualify as a straight-line full-throttle event.
    // filters out slow corner exits where throttle=100 at lower speeds.
    private static final int MIN_SPEED = 280;

    // minimum gear to qualify. lift & coast happens in 7th or 8th gear on straights,
    // not in lower gears during corner-exit acceleration.
    private static final int MIN_GEAR = 7;

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
                        // ex: throttle=100, speed=342, gear=8 -> match (monza main straight)
                        // ex: throttle=100, speed=180, gear=5 -> skip (corner exit)
                        return e.getThrottle() == 100
                                && e.getSpeed() > MIN_SPEED
                                && e.getNGear() >= MIN_GEAR;
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
                .within(Time.seconds(4));

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
                            br.getDate(), ft.getTrackStatus(),
                            ft.getSpeed(), ft.getNGear());
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
