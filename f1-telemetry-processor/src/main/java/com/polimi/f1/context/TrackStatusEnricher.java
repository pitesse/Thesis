package com.polimi.f1.context;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.events.TelemetryEvent;
import com.polimi.f1.events.TrackStatusEvent;

// enriches the per-driver telemetry stream with the current global track status.
// uses flink's broadcast state pattern: the sparse track status stream is broadcast
// to all parallel instances, each maintaining a local copy in a MapState.
// this avoids keying the global track status by driver (which would be semantically wrong)
// while still making it accessible inside a keyed context.
//
// why broadcast state (not a side input or lookup):
// flink streaming has no true "side inputs". broadcast state is the canonical pattern
// for joining a slowly-changing dimension (track status) to a high-frequency stream (telemetry).
public class TrackStatusEnricher
        extends KeyedBroadcastProcessFunction<String, TelemetryEvent, TrackStatusEvent, TelemetryEvent> {

    // state descriptor shared between broadcast and processing sides.
    // single key "current" holds the latest track status code.
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
        ctx.getBroadcastState(TRACK_STATUS_STATE).put("current", statusEvent.getStatus());
    }

    // called for each telemetry event (keyed side).
    // reads the current track status from broadcast state and attaches it to the event.
    // defaults to "1" (green/all clear) if no status change has been received yet.
    @Override
    public void processElement(
            TelemetryEvent event,
            KeyedBroadcastProcessFunction<String, TelemetryEvent, TrackStatusEvent, TelemetryEvent>.ReadOnlyContext ctx,
            Collector<TelemetryEvent> out) throws Exception {
        String status = ctx.getBroadcastState(TRACK_STATUS_STATE).get("current");
        if (status == null) {
            status = "1"; // default: green flag
        }
        event.setTrackStatus(status);
        out.collect(event);
    }
}
