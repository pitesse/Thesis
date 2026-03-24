package com.polimi.f1;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.input.TelemetryEvent;
import com.polimi.f1.model.input.TrackStatusEvent;
import com.polimi.f1.model.output.DropZoneAlert;
import com.polimi.f1.model.output.LiftCoastAlert;
import com.polimi.f1.model.output.MLFeatureRow;
import com.polimi.f1.model.output.PitStopEvaluationAlert;
import com.polimi.f1.model.output.PitSuggestionAlert;
import com.polimi.f1.model.output.RivalInfoAlert;
import com.polimi.f1.model.output.TireDropAlert;
import com.polimi.f1.operators.context.DrsTrainDetector;
import com.polimi.f1.operators.context.RivalIdentificationFunction;
import com.polimi.f1.operators.context.TrackStatusEnricher;
import com.polimi.f1.operators.groundtruth.PitStopEvaluator;
import com.polimi.f1.operators.realtime.DropZoneEvaluator;
import com.polimi.f1.operators.realtime.LiftCoastDetector;
import com.polimi.f1.operators.realtime.PitStrategyEvaluator;
import com.polimi.f1.operators.realtime.TireDropDetector;
import com.polimi.f1.utils.JsonDeserializer;
import com.polimi.f1.utils.JsonSerializer;

// main flink job: wires kafka sources, event-time watermarks, and all processing pipelines.
// consumes three topics: f1-telemetry (high-freq car data), f1-laps (per-lap summaries),
// f1-track-status (global track state changes).
//
// module a (context): track status broadcast, rival identification, drs train detection
// module b (ground truth): pit stop evaluation (success/failure classification)
// module c (real-time alerts): lift & coast cep, tire drop detection, drop zone analysis
public class F1StreamingJob {

    private static final String KAFKA_BOOTSTRAP = "kafka:29092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpointing every 10s: required for FileSink to commit in-progress part files
        // and for KafkaSink to guarantee at-least-once delivery to the alerts topic.
        env.enableCheckpointing(10_000);
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage("file:///opt/flink/data_lake/checkpoints"));
        //TODO enabling these slow down the dashboard and json dumping significantly, investigate further and consider re-enabling if they can be optimized.
        // env.getCheckpointConfig().setCheckpointingMode(
        //         org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().enableUnalignedCheckpoints();
        // env.setStateBackend(new org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend(true));

        // kafka sources
        // high-frequency car telemetry (~4 Hz per driver)
        KafkaSource<String> telemetrySourceFromKafka = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics("f1-telemetry")
                .setGroupId("f1-telemetry-processor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // lap completion events (~1 per driver per ~80s)
        KafkaSource<String> lapSourceFromKafka = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics("f1-laps")
                .setGroupId("f1-lap-processor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // global track status changes (sparse, ~1-5 per race)
        KafkaSource<String> trackStatusSourceFromKafka = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics("f1-track-status")
                .setGroupId("f1-track-status-processor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // raw streams from kafka (Json strings). separate streams to allow independent watermarking strategies per event type.
        DataStream<String> rawTelemetry = env
                .fromSource(telemetrySourceFromKafka, WatermarkStrategy.noWatermarks(), "Kafka: f1-telemetry");
        DataStream<String> rawLaps = env
                .fromSource(lapSourceFromKafka, WatermarkStrategy.noWatermarks(), "Kafka: f1-laps");
        DataStream<String> rawTrackStatus = env
                .fromSource(trackStatusSourceFromKafka, WatermarkStrategy.noWatermarks(), "Kafka: f1-track-status");

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
        // track status uses withIdleness(30s) because events are sparse (minutes apart),
        // without idleness the watermark would stall the entire pipeline between status changes.
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

        // a2: rival identification + ml feature export
        // window all lap events by lap number, identify the car ahead/behind for each driver.
        // session window with 30s gap: all drivers' events for the same lap arrive within seconds.
        // SingleOutputStreamOperator (not DataStream) to access the ml features side output.
        SingleOutputStreamOperator<RivalInfoAlert> rivalResult = lapWithWatermarks
                .keyBy(F1StreamingJob::raceLapKey)
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(30)))
                .process(new RivalIdentificationFunction())
                .name("Rival Identification");

        // output stream of RivalInfoAlert (identified rivals and gaps per driver per lap)
        DataStream<RivalInfoAlert> rivalStream = rivalResult;

        // ml feature side output: denormalized feature row per driver per lap,
        // combining timing/tire data with gap context for offline model training
        DataStream<MLFeatureRow> mlFeatures = rivalResult
                .getSideOutput(RivalIdentificationFunction.ML_FEATURES_TAG);

        // a3: drs train detection 
        // re-window the rival info by lap number to detect contiguous groups within 1s gap
        DataStream<String> drsTrainAlerts = rivalStream
                .keyBy(alert -> raceLapKey(alert.getRace(), alert.getLapNumber()))
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(10)))
                .process(new DrsTrainDetector())
                .name("DRS Train Detection");

        // module b: ground truth
        // pit stop evaluation: classifies each pit stop as success (undercut/defend) or failure
        // by comparing the pitting driver against their net rival (car directly ahead at pit time).
        // keyed by "RACE" for global field visibility (needs to see all drivers' positions).
        DataStream<PitStopEvaluationAlert> pitEvals = lapWithWatermarks
                .keyBy(F1StreamingJob::raceKey)
                .process(new PitStopEvaluator())
                .name("Pit Stop Evaluation");

        // module c: real-time alerts
        // c1: lift & coast detection (cep)
        // uses the enriched stream (carries track status context).
        // pattern, select, and dedup logic encapsulated in LiftCoastDetector.
        DataStream<LiftCoastAlert> liftCoastAlerts = LiftCoastDetector.detect(enrichedTelemetry);

        //  c2: tire drop detection 
        // detects when rolling 3-lap average degrades beyond 1.5s from stint best
        DataStream<TireDropAlert> tireDropAlerts = lapWithWatermarks
                .keyBy(LapEvent::getDriver)
                .process(new TireDropDetector())
                .name("Tire Drop Detection");

        //  c3: drop zone analysis (replaces pit window detector)
        // computes physical race emergence position after pit stop by walking the full
        // position ladder. bridges the net race (classification rivals) and physical race
        // (on-track traffic after pitting). only evaluates drivers with tyre life >= 8.
        // leader-driven trigger: when P1 finishes lap N, lap N-1 is guaranteed complete
        // for the entire field. no timers, no watermark dependency, just pure race physics.
        DataStream<DropZoneAlert> dropZoneAlerts = lapWithWatermarks
                .keyBy(F1StreamingJob::raceKey)
                .process(new DropZoneEvaluator())
                .name("Drop Zone Analysis");

        // c4: pit strategy evaluation (continuous fuzzy-logic pit desirability scoring)
        // computes a 0.0-100.0 continuous score per driver per lap based on pace degradation
        // (power 1.5 curve), traffic (linear interpolation), urgency (quadratic ramp),
        // strategy penalty (compound feasibility), and track status (crisp +60 for SC/VSC).
        // uses broadcast state for SC/VSC urgency: when safety car deploys, immediately
        // re-evaluates all drivers without waiting for next lap completion (~80s latency).
        // keyed by "RACE" for global position-ladder visibility (same as drop zone).
        BroadcastStream<TrackStatusEvent> broadcastForStrategy
                = trackStatusWithWatermarks.broadcast(PitStrategyEvaluator.TRACK_STATUS_STATE);

        DataStream<PitSuggestionAlert> pitSuggestions = lapWithWatermarks
                .keyBy(F1StreamingJob::raceKey)
                .connect(broadcastForStrategy)
                .process(new PitStrategyEvaluator())
                .name("Pit Strategy Evaluation");

        // sinks (print to taskmanager stdout for development)
        // pitEvals and tireDropAlerts are persisted via FileSink and routed via KafkaSink,
        // so their print sinks are removed to avoid redundant stdout noise.
        liftCoastAlerts.map(LiftCoastAlert::toString).returns(String.class).print().name("Lift & Coast Alerts");
        dropZoneAlerts.map(DropZoneAlert::toString).returns(String.class).print().name("Drop Zone Alerts");
        drsTrainAlerts.print().name("DRS Train Alerts");
        pitSuggestions.map(PitSuggestionAlert::toString).returns(String.class)
                .print().name("Pit Suggestions");

        // file sinks (jsonl for ml dataset generation)
        // persists ground truth and alert streams to disk as newline-delimited json.
        // rolling policy: new file every 15s or 10 MB, whichever comes first.
        // inactivity interval (15s) ensures files are finalized promptly when the simulation ends.
        // output lands in /opt/flink/data_lake/ inside the container, mapped to ./data_lake/ on the host.
        DataStream<String> pitEvalsJsonStream = pitEvals
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Pit Evaluations (JSONL)");

        DataStream<String> tireDropsJsonStream = tireDropAlerts
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Tire Drops (JSONL)");

        FileSink<String> pitEvalSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/pit_evals"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(buildCsvRollingPolicy())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("pit-eval")
                        .withPartSuffix(".jsonl")
                        .build())
                .build();

        FileSink<String> tireDropSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/tire_drops"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(buildCsvRollingPolicy())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("tire-drop")
                        .withPartSuffix(".jsonl")
                        .build())
                .build();

        pitEvalsJsonStream.sinkTo(pitEvalSink).name("FileSink: Pit Evaluations").uid("sink-pit-evals");
        tireDropsJsonStream.sinkTo(tireDropSink).name("FileSink: Tire Drops").uid("sink-tire-drops");

        // lift & coast jsonl sink
        DataStream<String> liftCoastJsonStream = liftCoastAlerts
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Lift & Coast (JSONL)");

        FileSink<String> liftCoastSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/lift_coast"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(buildCsvRollingPolicy())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("lift-coast")
                        .withPartSuffix(".jsonl")
                        .build())
                .build();

        liftCoastJsonStream.sinkTo(liftCoastSink).name("FileSink: Lift & Coast").uid("sink-lift-coast");

        // drop zone jsonl sink (replaces pit window)
        DataStream<String> dropZoneJsonStream = dropZoneAlerts
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Drop Zones (JSONL)");

        FileSink<String> dropZoneSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/drop_zones"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(buildCsvRollingPolicy())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("drop-zone")
                        .withPartSuffix(".jsonl")
                        .build())
                .build();

        dropZoneJsonStream.sinkTo(dropZoneSink).name("FileSink: Drop Zones").uid("sink-drop-zones");

        // pit suggestions jsonl sink
        DataStream<String> pitSuggestionsJsonStream = pitSuggestions
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Pit Suggestions (JSONL)");

        FileSink<String> pitSuggestionsSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/pit_suggestions"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(buildCsvRollingPolicy())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("pit-suggestion")
                        .withPartSuffix(".jsonl")
                        .build())
                .build();

        pitSuggestionsJsonStream.sinkTo(pitSuggestionsSink)
                .name("FileSink: Pit Suggestions").uid("sink-pit-suggestions");

        // ml features jsonl sink (denormalized per-lap feature rows for model training)
        DataStream<String> mlFeaturesJsonStream = mlFeatures
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize ML Features (JSONL)");

        FileSink<String> mlFeaturesSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/ml_features"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(buildCsvRollingPolicy())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("ml-features")
                        .withPartSuffix(".jsonl")
                        .build())
                .build();

        mlFeaturesJsonStream.sinkTo(mlFeaturesSink).name("FileSink: ML Features").uid("sink-ml-features");

        // kafka sink (route alerts to dashboard via f1-alerts topic)
        // the dashboard subscribes to f1-alerts to display live strategic alerts.
        // all three alert types are serialized to json, unioned into a single stream,
        // and published to kafka. at-least-once delivery is sufficient for display purposes.
        DataStream<String> liftCoastJson = liftCoastAlerts
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Lift & Coast");

        DataStream<String> dropZoneJson = dropZoneAlerts
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Drop Zones (Kafka)");

        DataStream<String> tireDropsJson = tireDropAlerts
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Tire Drops (Kafka)");

        DataStream<String> pitSuggestionsJson = pitSuggestions
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Pit Suggestions (Kafka)");

        DataStream<String> pitEvalsJson = pitEvals
                .map(new JsonSerializer<>())
                .returns(String.class)
                .name("Serialize Pit Evaluations (Kafka)");

        DataStream<String> allAlerts = liftCoastJson
                .union(tireDropsJson, dropZoneJson, pitSuggestionsJson, pitEvalsJson);

        KafkaSink<String> alertsSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("f1-alerts")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        allAlerts.sinkTo(alertsSink).name("KafkaSink: Alerts").uid("sink-kafka-alerts");

        // debug text sink: consolidated file of all alerts for inspection without
        // scrolling through docker logs. writes json lines (same format as kafka sink).
        FileSink<String> debugSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/debug_alerts"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(15))
                                .withInactivityInterval(Duration.ofSeconds(15))
                                .withMaxPartSize(MemorySize.ofMebiBytes(10))
                                .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("debug-alerts")
                        .withPartSuffix(".txt")
                        .build())
                .build();

        allAlerts.sinkTo(debugSink).name("FileSink: Debug Alerts").uid("sink-debug-alerts");

        // development: log lap events and track status for visibility
        lapWithWatermarks.map(lap -> "LAP | " + lap.toString()).returns(String.class)
                .print().name("Lap Events");
        trackStatusWithWatermarks.map(ts -> "TRACK STATUS | " + ts.toString()).returns(String.class)
                .print().name("Track Status Events");
        rivalStream.map(r -> "RIVAL | " + r.toString()).returns(String.class)
                .print().name("Rival Info");

        env.execute("F1 Strategy Operations");
    }

    // creates a standardized rolling policy for csv data lake outputs.
    // rolls files every 15s, after 15s inactivity, or when reaching 10 MB.
    // consistent across all ml dataset sinks for predictable file organization.
    private static DefaultRollingPolicy buildCsvRollingPolicy() {
        return DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofSeconds(15))
                .withInactivityInterval(Duration.ofSeconds(15))
                .withMaxPartSize(MemorySize.ofMebiBytes(10))
                .build();
    }

        private static String raceKey(LapEvent lap) {
                return lap.getRace() != null ? lap.getRace() : "UNKNOWN_RACE";
        }

        private static String raceLapKey(LapEvent lap) {
                return raceLapKey(raceKey(lap), lap.getLapNumber());
        }

        private static String raceLapKey(String race, int lapNumber) {
                return race + "|" + lapNumber;
        }

}
