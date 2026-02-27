package com.polimi.f1;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

// module c — real-time alert: lift & coast detection via flink cep.
// detects fuel-saving behavior by matching a pedal input sequence
// (full throttle -> throttle release -> braking) within a short window.
public class F1StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(F1StreamingJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // consume raw json strings from the f1-telemetry topic.
        // starts from latest offset — only processes events arriving after job submission.
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("f1-telemetry")
                .setGroupId("f1-telemetry-processor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .name("F1 Telemetry Kafka Source");

        // deserialize json to TelemetryEvent pojo via jackson.
        // flatMap instead of map so malformed records are silently dropped (logged, not thrown).
        DataStream<TelemetryEvent> telemetryStream = rawStream
                .flatMap(new RichFlatMapFunction<String, TelemetryEvent>() {
                    private transient ObjectMapper mapper;

                    @Override
                    public void open(Configuration parameters) {
                        mapper = new ObjectMapper();
                    }

                    @Override
                    public void flatMap(String value, Collector<TelemetryEvent> out) {
                        try {
                            out.collect(mapper.readValue(value, TelemetryEvent.class));
                        } catch (JsonProcessingException e) {
                            LOG.warn("Skipping malformed telemetry record: {}", value, e);
                        }
                    }
                })
                .name("JSON Deserializer");

        // switch from processing time to event time using the "Date" field from the json payload.
        // 5-second bounded out-of-orderness accounts for network jitter and the python producer's
        // per-row sleep not being perfectly synchronized across drivers.
        DataStream<TelemetryEvent> withWatermarks = telemetryStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TelemetryEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getEventTimeMillis())
                )
                .name("Watermark Assignment");

        // partition by driver, cep patterns must be evaluated per-driver, not globally
        KeyedStream<TelemetryEvent, String> keyedStream =
                withWatermarks.keyBy(TelemetryEvent::getDriver);

        // lift & coast cep pattern: detects fuel-saving maneuvers.
        // sequence: throttle=100 -> throttle=0 -> brake>0, all within 2 seconds.
        // followedBy (not next) allows non-relevant events between steps, real telemetry
        // has intermediate throttle values and gear changes between pedal transitions.
        Pattern<TelemetryEvent, ?> liftAndCoastPattern = Pattern
                .<TelemetryEvent>begin("full-throttle")
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

        PatternStream<TelemetryEvent> patternStream =
                CEP.pattern(keyedStream, liftAndCoastPattern);

        // emit a formatted alert string for each matched lift & coast sequence
        DataStream<String> alerts = patternStream.select(
                new PatternSelectFunction<TelemetryEvent, String>() {
                    @Override
                    public String select(Map<String, List<TelemetryEvent>> match) {
                        TelemetryEvent ft = match.get("full-throttle").get(0);
                        TelemetryEvent li = match.get("lift").get(0);
                        TelemetryEvent br = match.get("coast-brake").get(0);
                        return String.format(
                                "LIFT & COAST | Driver: %s | Throttle@%s -> Lift@%s -> Brake@%s",
                                ft.getDriver(), ft.getDate(), li.getDate(), br.getDate());
                    }
                }
        );

        alerts.print().name("Lift & Coast Alerts");

        env.execute("F1 Strategy Job - Lift & Coast Detection");
    }
}
