package com.polimi.f1;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.polimi.f1.alerts.DropZoneEvaluator;
import com.polimi.f1.alerts.LiftCoastDetector;
import com.polimi.f1.alerts.PitStrategyEvaluator;
import com.polimi.f1.alerts.TireDropDetector;
import com.polimi.f1.context.DrsTrainDetector;
import com.polimi.f1.context.RivalIdentificationFunction;
import com.polimi.f1.context.TrackStatusEnricher;
import com.polimi.f1.events.LapEvent;
import com.polimi.f1.events.TelemetryEvent;
import com.polimi.f1.events.TrackStatusEvent;
import com.polimi.f1.groundtruth.PitStopEvaluator;
import com.polimi.f1.model.DropZoneAlert;
import com.polimi.f1.model.LiftCoastAlert;
import com.polimi.f1.model.MLFeatureRow;
import com.polimi.f1.model.PitStopEvaluationAlert;
import com.polimi.f1.model.PitSuggestionAlert;
import com.polimi.f1.model.RivalInfoAlert;
import com.polimi.f1.model.TireDropAlert;

// main flink job: wires kafka sources, event-time watermarks, and all processing pipelines.
// consumes three topics: f1-telemetry (high-freq car data), f1-laps (per-lap summaries),
// f1-track-status (global track state changes).
//
// module a (context): track status broadcast, rival identification, drs train detection
// module b (ground truth): pit stop evaluation (success/failure classification)
// module c (real-time alerts): lift & coast cep, tire drop detection, drop zone analysis
public class F1StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(F1StreamingJob.class);
    private static final String KAFKA_BOOTSTRAP = "kafka:29092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpointing every 5s: required for FileSink to commit in-progress part files
        // and for KafkaSink to guarantee at-least-once delivery to the alerts topic.
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage("file:///opt/flink/data_lake/checkpoints"));

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
                .keyBy(LapEvent::getLapNumber)
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .process(new RivalIdentificationFunction())
                .name("Rival Identification");

        DataStream<RivalInfoAlert> rivalStream = rivalResult;

        // ml feature side output: denormalized feature row per driver per lap,
        // combining timing/tire data with gap context for offline model training
        DataStream<MLFeatureRow> mlFeatures = rivalResult
                .getSideOutput(RivalIdentificationFunction.ML_FEATURES_TAG);

        // a3: drs train detection 
        // re-window the rival info by lap number to detect contiguous groups within 1s gap
        DataStream<String> drsTrainAlerts = rivalStream
                .keyBy(RivalInfoAlert::getLapNumber)
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new DrsTrainDetector())
                .name("DRS Train Detection");

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
                .keyBy(e -> "RACE")
                .process(new DropZoneEvaluator())
                .name("Drop Zone Analysis");

        // c4: pit strategy evaluation (fuzzy-logic pit desirability scoring)
        // computes a 0-100 score per driver per lap based on pace degradation,
        // track status, traffic after pit, and compound feasibility.
        // keyed by "RACE" for global position-ladder visibility (same as drop zone).
        DataStream<PitSuggestionAlert> pitSuggestions = lapWithWatermarks
                .keyBy(e -> "RACE")
                .process(new PitStrategyEvaluator())
                .name("Pit Strategy Evaluation");

        // module b: ground truth
        // pit stop evaluation: classifies each pit stop as success (undercut/defend) or failure
        // by comparing position before and after, using a 3-lap post-pit observation window.
        DataStream<PitStopEvaluationAlert> pitEvals = lapWithWatermarks
                .keyBy(LapEvent::getDriver)
                .process(new PitStopEvaluator())
                .name("Pit Stop Evaluation");

        // sinks (print to taskmanager stdout for development)
        // pitEvals and tireDropAlerts are persisted via FileSink and routed via KafkaSink,
        // so their print sinks are removed to avoid redundant stdout noise.
        liftCoastAlerts.map(LiftCoastAlert::toString).returns(String.class).print().name("Lift & Coast Alerts");
        dropZoneAlerts.map(DropZoneAlert::toString).returns(String.class).print().name("Drop Zone Alerts");
        drsTrainAlerts.print().name("DRS Train Alerts");
        pitSuggestions.map(PitSuggestionAlert::toString).returns(String.class)
                .print().name("Pit Suggestions");

        // file sinks (csv for ml dataset generation)
        // persists ground truth and alert streams to disk as csv (one row per event, header-prefixed).
        // rolling policy: new file every 5 min or 128 MB, whichever comes first.
        // inactivity interval (3 min) ensures files are finalized promptly during low-traffic periods.
        // output lands in /opt/flink/data_lake/ inside the container, mapped to ./data_lake/ on the host.
        // serialize pojos to csv rows via toCsvRow(), csv header emitted once via CsvHeaderMapper
        DataStream<String> pitEvalsCsv = pitEvals
                .map(new CsvHeaderMapper<>(PitStopEvaluationAlert.CSV_HEADER, PitStopEvaluationAlert::toCsvRow))
                .returns(String.class)
                .name("Serialize Pit Evaluations (CSV)");

        DataStream<String> tireDropsCsv = tireDropAlerts
                .map(new CsvHeaderMapper<>(TireDropAlert.CSV_HEADER, TireDropAlert::toCsvRow))
                .returns(String.class)
                .name("Serialize Tire Drops (CSV)");

        FileSink<String> pitEvalSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/pit_evals"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(3))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("pit-eval")
                        .withPartSuffix(".csv")
                        .build())
                .build();

        FileSink<String> tireDropSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/tire_drops"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(3))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("tire-drop")
                        .withPartSuffix(".csv")
                        .build())
                .build();

        pitEvalsCsv.sinkTo(pitEvalSink).name("FileSink: Pit Evaluations").uid("sink-pit-evals");
        tireDropsCsv.sinkTo(tireDropSink).name("FileSink: Tire Drops").uid("sink-tire-drops");

        // lift & coast csv sink
        DataStream<String> liftCoastCsv = liftCoastAlerts
                .map(new CsvHeaderMapper<>(LiftCoastAlert.CSV_HEADER, LiftCoastAlert::toCsvRow))
                .returns(String.class)
                .name("Serialize Lift & Coast (CSV)");

        FileSink<String> liftCoastSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/lift_coast"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(3))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("lift-coast")
                        .withPartSuffix(".csv")
                        .build())
                .build();

        liftCoastCsv.sinkTo(liftCoastSink).name("FileSink: Lift & Coast").uid("sink-lift-coast");

        // drop zone csv sink (replaces pit window)
        DataStream<String> dropZoneCsv = dropZoneAlerts
                .map(new CsvHeaderMapper<>(DropZoneAlert.CSV_HEADER, DropZoneAlert::toCsvRow))
                .returns(String.class)
                .name("Serialize Drop Zones (CSV)");

        FileSink<String> dropZoneSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/drop_zones"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(3))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("drop-zone")
                        .withPartSuffix(".csv")
                        .build())
                .build();

        dropZoneCsv.sinkTo(dropZoneSink).name("FileSink: Drop Zones").uid("sink-drop-zones");

        // pit suggestions csv sink
        DataStream<String> pitSuggestionsCsv = pitSuggestions
                .map(new CsvHeaderMapper<>(PitSuggestionAlert.CSV_HEADER, PitSuggestionAlert::toCsvRow))
                .returns(String.class)
                .name("Serialize Pit Suggestions (CSV)");

        FileSink<String> pitSuggestionsSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/pit_suggestions"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(3))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("pit-suggestion")
                        .withPartSuffix(".csv")
                        .build())
                .build();

        pitSuggestionsCsv.sinkTo(pitSuggestionsSink)
                .name("FileSink: Pit Suggestions").uid("sink-pit-suggestions");

        // ml features csv sink (denormalized per-lap feature rows for model training)
        DataStream<String> mlFeaturesCsv = mlFeatures
                .map(new CsvHeaderMapper<>(MLFeatureRow.CSV_HEADER, MLFeatureRow::toCsvRow))
                .returns(String.class)
                .name("Serialize ML Features (CSV)");

        FileSink<String> mlFeaturesSink = FileSink
                .forRowFormat(new Path("/opt/flink/data_lake/ml_features"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(3))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("ml-features")
                        .withPartSuffix(".csv")
                        .build())
                .build();

        mlFeaturesCsv.sinkTo(mlFeaturesSink).name("FileSink: ML Features").uid("sink-ml-features");

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

        DataStream<String> allAlerts = liftCoastJson
                .union(tireDropsJson, dropZoneJson, pitSuggestionsJson);

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
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(3))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
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

    // generic jackson serializer for kafka sinks, mirrors JsonDeserializer.
    // converts pojos to json format (one compact json object per line).
    // ex: PitStopEvaluationAlert -> {"driver":"VER","pitLapNumber":15,"result":"SUCCESS_UNDERCUT",...}
    private static class JsonSerializer<T> extends RichMapFunction<T, String> {

        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public String map(T value) throws Exception {
            return mapper.writeValueAsString(value);
        }
    }

    // emits a csv header as the first row, then delegates to toCsvRow() for data rows.
    // uses a boolean flag to emit the header exactly once per parallel instance.
    // ex output: "driver,pitLapNumber,...\nVER,15,..."
    private static class CsvHeaderMapper<T> implements MapFunction<T, String> {

        private final String header;
        private final SerializableToCsvRow<T> toCsvRow;
        private boolean headerEmitted = false;

        CsvHeaderMapper(String header, SerializableToCsvRow<T> toCsvRow) {
            this.header = header;
            this.toCsvRow = toCsvRow;
        }

        @Override
        public String map(T value) throws Exception {
            if (!headerEmitted) {
                headerEmitted = true;
                return header + "\n" + toCsvRow.apply(value);
            }
            return toCsvRow.apply(value);
        }
    }

    // functional interface for toCsvRow method references, must be serializable for flink
    @FunctionalInterface
    private interface SerializableToCsvRow<T> extends java.io.Serializable {

        String apply(T value);
    }

}
