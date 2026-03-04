package com.polimi.f1;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.polimi.f1.alerts.PitWindowDetector;
import com.polimi.f1.alerts.TireDropDetector;
import com.polimi.f1.context.DrsTrainDetector;
import com.polimi.f1.context.RivalIdentificationFunction;
import com.polimi.f1.context.TrackStatusEnricher;
import com.polimi.f1.events.LapEvent;
import com.polimi.f1.events.TelemetryEvent;
import com.polimi.f1.events.TrackStatusEvent;
import com.polimi.f1.groundtruth.PitStopEvaluator;
import com.polimi.f1.model.LiftCoastAlert;
import com.polimi.f1.model.PitStopEvaluationAlert;
import com.polimi.f1.model.PitWindowAlert;
import com.polimi.f1.model.RivalInfoAlert;
import com.polimi.f1.model.TireDropAlert;

// main flink job: wires kafka sources, event-time watermarks, and all processing pipelines.
// consumes three topics: f1-telemetry (high-freq car data), f1-laps (per-lap summaries),
// f1-track-status (global track state changes).
//
// module a (context): track status broadcast, rival identification, drs train detection
// module b (ground truth): pit stop evaluation (success/failure classification)
// module c (real-time alerts): lift & coast cep, tire drop detection, open box window
public class F1StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(F1StreamingJob.class);
    private static final String KAFKA_BOOTSTRAP = "kafka:29092";

    public static void main(String[] args) throws Exception {
        // parse cli arguments, ex: --pit-loss 22.0 for circuits with shorter pit lane delta
        ParameterTool params = ParameterTool.fromArgs(args);
        double pitLoss = params.getDouble("pit-loss", 25.0);
        LOG.info("Configuration: pit-loss={}", pitLoss);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka sources
        // high-frequency car telemetry (~4 Hz per driver)
        KafkaSource<String> telemetrySource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics("f1-telemetry")
                .setGroupId("f1-telemetry-processor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // lap completion events (~1 per driver per ~80s)
        KafkaSource<String> lapSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics("f1-laps")
                .setGroupId("f1-lap-processor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // global track status changes (sparse, ~1-5 per race)
        KafkaSource<String> trackStatusSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics("f1-track-status")
                .setGroupId("f1-track-status-processor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // raw streams + deserialization
        DataStream<String> rawTelemetry = env
                .fromSource(telemetrySource, WatermarkStrategy.noWatermarks(), "Kafka: f1-telemetry");
        DataStream<String> rawLaps = env
                .fromSource(lapSource, WatermarkStrategy.noWatermarks(), "Kafka: f1-laps");
        DataStream<String> rawTrackStatus = env
                .fromSource(trackStatusSource, WatermarkStrategy.noWatermarks(), "Kafka: f1-track-status");

        // flatMap drops malformed records (logged, not thrown) to prevent pipeline failures.
        // .returns() is required because generic JsonDeserializer<T> loses type info at runtime (erasure)
        DataStream<TelemetryEvent> telemetryStream = rawTelemetry
                .flatMap(new JsonDeserializer<>(TelemetryEvent.class))
                .returns(TelemetryEvent.class)
                .name("Deserialize Telemetry");
        DataStream<LapEvent> lapStream = rawLaps
                .flatMap(new JsonDeserializer<>(LapEvent.class))
                .returns(LapEvent.class)
                .name("Deserialize Laps");
        DataStream<TrackStatusEvent> trackStatusStream = rawTrackStatus
                .flatMap(new JsonDeserializer<>(TrackStatusEvent.class))
                .returns(TrackStatusEvent.class)
                .name("Deserialize Track Status");

        // watermark assignment
        // 5-second bounded out-of-orderness for telemetry and laps accounts for network jitter
        // and the python producer's per-row sleep not being perfectly synchronized across drivers.
        DataStream<TelemetryEvent> telemetryWithWatermarks = telemetryStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TelemetryEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getEventTimeMillis())
                )
                .name("Telemetry Watermarks");

        DataStream<LapEvent> lapWithWatermarks = lapStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LapEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getEventTimeMillis())
                )
                .name("Lap Watermarks");
        // track status uses withIdleness(30s) because events are sparse (minutes apart),
        // without idleness the watermark would stall the entire pipeline between status changes.
        DataStream<TrackStatusEvent> trackStatusWithWatermarks = trackStatusStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrackStatusEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getEventTimeMillis())
                                .withIdleness(Duration.ofSeconds(30))
                )
                .name("Track Status Watermarks");

        // module a: context awareness
        // a1: track status enrichment (broadcast state)
        // broadcast the sparse track status stream to all parallel telemetry instances.
        // each instance maintains a local copy of the current track status ("1"=green default).
        BroadcastStream<TrackStatusEvent> broadcastTrackStatus
                = trackStatusWithWatermarks.broadcast(TrackStatusEnricher.TRACK_STATUS_STATE);

        DataStream<TelemetryEvent> enrichedTelemetry = telemetryWithWatermarks
                .keyBy(TelemetryEvent::getDriver)
                .connect(broadcastTrackStatus)
                .process(new TrackStatusEnricher())
                .name("Track Status Enrichment");

        // a2: rival identification 
        // window all lap events by lap number, identify the car ahead/behind for each driver.
        // session window with 30s gap: all drivers' events for the same lap arrive within seconds.
        DataStream<RivalInfoAlert> rivalStream = lapWithWatermarks
                .keyBy(LapEvent::getLapNumber)
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(30))) // maybe review the 30s logic
                .process(new RivalIdentificationFunction())
                .name("Rival Identification"); 

        // a3: drs train detection 
        // re-window the rival info by lap number to detect contiguous groups within 1s gap
        DataStream<String> drsTrainAlerts = rivalStream
                .keyBy(RivalInfoAlert::getLapNumber)
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(10)))
                .process(new DrsTrainDetector())
                .name("DRS Train Detection");

        // module c: real-time alerts
        // c1: lift & coast detection (cep) 
        // uses the enriched stream (carries track status context)
        KeyedStream<TelemetryEvent, String> keyedTelemetry
                = enrichedTelemetry.keyBy(TelemetryEvent::getDriver);

        // lift & coast cep pattern: detects fuel-saving maneuvers.
        // sequence: throttle=100 -> throttle=0 -> brake>0, all within 2 seconds.
        // followedBy (not next) allows non-relevant events between steps, real telemetry
        // has intermediate throttle values and gear changes between pedal transitions.
        // skipPastLastEvent prevents combinatorial explosion: at 4Hz, multiple consecutive
        // full-throttle samples would each independently match the same lift+brake event.
        // after a match, scanning resumes after the brake event, yielding one alert per braking zone.
        Pattern<TelemetryEvent, ?> liftAndCoastPattern = Pattern // TODO move this to its own class for clarity and reuse 
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

        PatternStream<TelemetryEvent> patternStream
                = CEP.pattern(keyedTelemetry, liftAndCoastPattern);

        DataStream<LiftCoastAlert> rawLiftCoastAlerts = patternStream.select((Map<String, List<TelemetryEvent>> match) -> {
            TelemetryEvent ft = match.get("full-throttle").get(0);
            TelemetryEvent li = match.get("lift").get(0);
            TelemetryEvent br = match.get("coast-brake").get(0);
            return new LiftCoastAlert(
                    ft.getDriver(), ft.getDate(), li.getDate(),
                    br.getDate(), ft.getTrackStatus());
        });

        // the cep's followedBy with relaxed contiguity causes N consecutive throttle=100
        // samples to each independently match the same lift+brake event, producing N alerts
        // for the same braking zone. this function suppresses duplicates by comparing the
        // brake timestamp (dedup key) against the last emitted alert for each driver.
        DataStream<LiftCoastAlert> liftCoastAlerts = rawLiftCoastAlerts
                .keyBy(LiftCoastAlert::getDriver)
                .process(new LiftCoastDedup())
                .name("Lift & Coast Dedup");

        //  c2: tire drop detection 
        // detects when rolling 3-lap average degrades beyond 1.5s from stint best
        DataStream<TireDropAlert> tireDropAlerts = lapWithWatermarks
                .keyBy(LapEvent::getDriver)
                .process(new TireDropDetector())
                .name("Tire Drop Detection");

        //  c3: open box window (pit window) 
        // evaluates gap to car behind against dynamic threshold based on track status.
        // requires both rival info (gap data) and track status (threshold selection).
        BroadcastStream<TrackStatusEvent> pitWindowBroadcast
                = trackStatusWithWatermarks.broadcast(PitWindowDetector.TRACK_STATUS_STATE);

        DataStream<PitWindowAlert> pitWindowAlerts = rivalStream
                .keyBy(RivalInfoAlert::getDriver)
                .connect(pitWindowBroadcast)
                .process(new PitWindowDetector(pitLoss))
                .name("Pit Window Detection");

        // module b: ground truth
        // pit stop evaluation: classifies each pit stop as success (undercut/defend) or failure
        // by comparing position before and after, using a 3-lap post-pit observation window.
        DataStream<PitStopEvaluationAlert> pitEvals = lapWithWatermarks
                .keyBy(LapEvent::getDriver)
                .process(new PitStopEvaluator())
                .name("Pit Stop Evaluation");

        // sinks (print to taskmanager stdout for development)
        liftCoastAlerts.map(LiftCoastAlert::toString).returns(String.class).print().name("Lift & Coast Alerts");
        tireDropAlerts.map(TireDropAlert::toString).returns(String.class).print().name("Tire Drop Alerts");
        pitWindowAlerts.map(PitWindowAlert::toString).returns(String.class).print().name("Pit Window Alerts");
        drsTrainAlerts.print().name("DRS Train Alerts");
        pitEvals.map(PitStopEvaluationAlert::toString).returns(String.class).print().name("Pit Stop Evaluations");

        // development: log lap events and track status for visibility
        lapWithWatermarks.map(lap -> "LAP | " + lap.toString()).returns(String.class)
                .print().name("Lap Events");
        trackStatusWithWatermarks.map(ts -> "TRACK STATUS | " + ts.toString()).returns(String.class)
                .print().name("Track Status Events");
        rivalStream.map(r -> "RIVAL | " + r.toString()).returns(String.class)
                .print().name("Rival Info");

        env.execute("F1 Strategy Operations");
    }

    // generic jackson deserializer, reusable across all event types.
    // creates one ObjectMapper per parallel instance (not serializable, so transient + open()).
    private static class JsonDeserializer<T> extends RichFlatMapFunction<String, T> {

        private final Class<T> targetClass;
        private transient ObjectMapper mapper;

        JsonDeserializer(Class<T> targetClass) {
            this.targetClass = targetClass;
        }

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public void flatMap(String value, Collector<T> out) {
            try {
                out.collect(mapper.readValue(value, targetClass));
            } catch (JsonProcessingException e) {
                LOG.warn("Skipping malformed {} record: {}", targetClass.getSimpleName(), value, e);
            }
        }
    }

    // suppresses duplicate lift & coast alerts from the same braking zone.
    // cep with followedBy fires N matches for N consecutive throttle=100 samples that all
    // converge on the same lift+brake event. this function keeps only the first alert per
    // braking zone by comparing brake timestamps, any alert with the same brakeDate as the
    // previous one is a duplicate and gets dropped. 
    //TODO move this in a different file like tiredrop and pitwindow detectors for clarity and reuse
    private static class LiftCoastDedup extends KeyedProcessFunction<String, LiftCoastAlert, LiftCoastAlert> {

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
