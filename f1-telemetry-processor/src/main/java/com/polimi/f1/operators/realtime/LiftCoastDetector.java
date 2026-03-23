package com.polimi.f1.operators.realtime;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.model.input.TelemetryEvent;
import com.polimi.f1.model.output.LiftCoastAlert;

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
    // 250 km/h is track-agnostic: captures shorter straights at monaco/hungary (~260-270)
    // while still filtering corner-exit bursts below 250.
    private static final int MIN_SPEED = 250;

    // minimum gear for full-throttle phase. >= 6 catches circuits with shorter gear ratios
    // (monaco tops out at 7th on the tunnel straight) while filtering low-speed acceleration.
    private static final int MIN_GEAR = 6;

    // throttle >= 95% qualifies as full-throttle, allowing for 1-5% sensor noise.
    private static final int FULL_THROTTLE_MIN = 95;

    // throttle < 5% qualifies as lift-off, ignoring 1-2% pedal sensor noise.
    private static final int LIFT_THROTTLE_MAX = 5;

    // minimum coast duration (ms) between throttle lift-off and brake application.
    // at 4 Hz sampling, timestamps are spaced 250ms apart. a measured gap of 500ms (two samples)
    // could be a normal braking transition. 550ms threshold requires at least 3 samples of coast
    // (actual 750ms gap), confirming deliberate fuel-saving intent.
    // ex: lift@t=0, coast@t=250, coast@t=500, brake@t=750 -> 750ms >= 550ms -> pass
    private static final long MIN_COAST_DURATION_MS = 550;

    // opening laps with standing start chaos, cold tires, and traffic are non-representative.
    // filter telemetry from laps 1-5 before cep matching.
    private static final int MIN_LAP_FOR_DETECTION = 5;
    private static final int PATTERN_WINDOW_SECONDS = 4;

    private LiftCoastDetector() {
    }

    // encapsulates the full lift & coast pipeline: pattern definition, cep matching,
    // and deduplication. expects an enriched telemetry stream (with track status set).
    public static DataStream<LiftCoastAlert> detect(DataStream<TelemetryEvent> enrichedTelemetry) {
        // pre-filter: remove non-representative data before cep matching.
        // vsc/sc laps have artificially suppressed speeds that distort coast detection,
        // and opening laps (1-5) have chaotic throttle patterns from standing start and traffic.
        DataStream<TelemetryEvent> cleanTelemetry = enrichedTelemetry
                .filter(e -> {
                    String status = e.getTrackStatus();
                    return (status == null || "1".equals(status))
                            && e.getLapNumber() > MIN_LAP_FOR_DETECTION;
                })
                .name("Pre-filter: Green Flag & Post-Opening Laps");

        Pattern<TelemetryEvent, ?> liftAndCoastPattern = Pattern
                .<TelemetryEvent>begin("full-throttle", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<TelemetryEvent>() {
                    @Override
                    public boolean filter(TelemetryEvent e) {
                        return e.getThrottle() >= FULL_THROTTLE_MIN
                                && e.getSpeed() >= MIN_SPEED
                                && e.getNGear() >= MIN_GEAR
                                && e.getBrake() == 0;
                    }
                })
                .followedBy("lift")
                .where(new SimpleCondition<TelemetryEvent>() {
                    @Override
                    public boolean filter(TelemetryEvent e) {
                        // < LIFT_THROTTLE_MAX ignores 1-2% pedal sensor noise
                        return e.getThrottle() < LIFT_THROTTLE_MAX && e.getBrake() == 0;
                    }
                })
                .followedBy("coast-brake")
                .where(new SimpleCondition<TelemetryEvent>() {
                    @Override
                    public boolean filter(TelemetryEvent e) {
                        // FastF1 brake data is binary (1 = on, 0 = off)
                        return e.getBrake() == 1;
                    }
                })
                .within(Duration.ofSeconds(PATTERN_WINDOW_SECONDS));

        PatternStream<TelemetryEvent> patternStream = CEP.pattern(
                cleanTelemetry.keyBy(TelemetryEvent::getDriver),
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
                })
                // reject normal corner braking where the coast phase is too short.
                // ex: lift@13:05:12.003, brake@13:05:12.080 -> 77ms -> skip (corner)
                // ex: lift@13:05:12.003, brake@13:05:13.500 -> 1497ms -> keep (lift & coast)
                .filter(alert -> {
                    long liftMs = TelemetryEvent.parseEventTime(alert.getLiftDate());
                    long brakeMs = TelemetryEvent.parseEventTime(alert.getBrakeDate());
                    return (brakeMs - liftMs) >= MIN_COAST_DURATION_MS;
                })
                .name("Filter Corner Braking");

        return rawAlerts
                .keyBy(LiftCoastAlert::getDriver)
                .process(new LiftCoastDedup())
                .name("Lift & Coast Dedup");
    }

    // suppresses duplicate lift & coast alerts from the same braking zone.
    // cep with followedBy fires N matches for N consecutive throttle=100 samples that all
    // converge on the same lift+brake event. this function keeps only the first alert per
    // braking zone by comparing a composite key of lift and brake timestamps, any alert with
    // the same liftDate+brakeDate as the previous one is a duplicate and gets dropped.
    // composite key handles edge cases where different full-throttle start points produce
    // matches with identical brakeDate but different liftDates via relaxed contiguity.
    static class LiftCoastDedup extends KeyedProcessFunction<String, LiftCoastAlert, LiftCoastAlert> {

        private transient ValueState<String> lastCompositeKey;

        @Override
        public void open(OpenContext openContext) {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(2))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<String> dedupDesc
                    = new ValueStateDescriptor<>("lastLiftBrake", String.class);
            dedupDesc.enableTimeToLive(ttlConfig);
            lastCompositeKey = getRuntimeContext().getState(dedupDesc);
        }

        @Override
        public void processElement(LiftCoastAlert alert, Context ctx,
                Collector<LiftCoastAlert> out) throws Exception {
            String currentKey = alert.getLiftDate() + "|" + alert.getBrakeDate();
            String last = lastCompositeKey.value();
            if (!currentKey.equals(last)) {
                out.collect(alert);
                lastCompositeKey.update(currentKey);
            }
        }
    }
}
