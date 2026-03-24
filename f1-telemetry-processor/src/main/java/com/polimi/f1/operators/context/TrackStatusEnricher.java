package com.polimi.f1.operators.context;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.model.input.TelemetryEvent;
import com.polimi.f1.model.input.TrackStatusEvent;

// enriches the per-driver telemetry stream with the current global track status
// using flink's broadcast state pattern.
//
// the sparse track status stream is broadcast to all parallel telemetry instances,
// each maintaining a local copy in a MapState. this avoids keying the global track
// status by driver (semantically wrong) while still making it accessible in keyed context.
//
// why broadcast state (not a side input or lookup): flink streaming has no true
// "side inputs". broadcast state is the canonical pattern for joining a slowly-changing
// dimension (track status, ~1-5 changes per race) to a high-frequency stream (~4 Hz).
public class TrackStatusEnricher
        extends KeyedBroadcastProcessFunction<String, TelemetryEvent, TrackStatusEvent, TelemetryEvent> {

    private static final String CURRENT_STATUS_KEY = "current";
    private static final String DEFAULT_GREEN_STATUS = "1";

    // state descriptor shared between broadcast and processing sides. single key "current".
    public static final MapStateDescriptor<String, String> TRACK_STATUS_STATE
            = new MapStateDescriptor<>(
                    "track-status",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
            );

    // called for each incoming track status change (broadcast side).
    // updates the broadcast state so all parallel telemetry processors see the new status.
    @Override
    public void processBroadcastElement(
            TrackStatusEvent statusEvent,
            KeyedBroadcastProcessFunction<String, TelemetryEvent, TrackStatusEvent, TelemetryEvent>.Context ctx,
            Collector<TelemetryEvent> out) throws Exception {
        if (statusEvent != null && statusEvent.getStatus() != null) {
            ctx.getBroadcastState(TRACK_STATUS_STATE).put(CURRENT_STATUS_KEY, statusEvent.getStatus());
        }
    }

    // called for each telemetry event (keyed side).
    // reads the current track status from broadcast state and attaches it to the event.
    // defaults to "1" (green/all clear) if no status change has been received yet.
    @Override
    public void processElement(
            TelemetryEvent event,
            KeyedBroadcastProcessFunction<String, TelemetryEvent, TrackStatusEvent, TelemetryEvent>.ReadOnlyContext ctx,
            Collector<TelemetryEvent> out) throws Exception {
        String status = ctx.getBroadcastState(TRACK_STATUS_STATE).get(CURRENT_STATUS_KEY);
        if (status == null) {
            status = DEFAULT_GREEN_STATUS; // default: green flag
        }
        event.setTrackStatus(status);
        out.collect(event);
    }
}
