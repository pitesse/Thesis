package com.polimi.f1.alerts;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.events.TrackStatusEvent;
import com.polimi.f1.model.PitWindowAlert;
import com.polimi.f1.model.RivalInfoAlert;

// evaluates whether a driver can pit without losing position based on gap to car behind.
// the threshold adapts dynamically based on track status and the track-specific pit loss
// times embedded in each event by the python producer:
//   green (status "1"): pitLoss from event (circuit-specific green-flag pit delta)
//   vsc   (status "6","7"): vscPitLoss from event (reduced delta under vsc)
//   sc    (status "4"): scPitLoss from event (minimal delta, field bunched up)
//   yellow/red: suppressed, pit lane may be closed or unsafe
//
// uses broadcast state to receive track status updates (same pattern as TrackStatusEnricher).
public class PitWindowDetector
        extends KeyedBroadcastProcessFunction<String, RivalInfoAlert, TrackStatusEvent, PitWindowAlert> {

    // fallback defaults if the event fields are null (e.g., legacy data without enrichment)
    private static final double DEFAULT_GREEN = 22.0;
    private static final double DEFAULT_VSC = 14.0;
    private static final double DEFAULT_SC = 11.0;

    // reuse the same state descriptor as TrackStatusEnricher for consistency
    public static final MapStateDescriptor<String, String> TRACK_STATUS_STATE =
            new MapStateDescriptor<>(
                    "pit-window-track-status",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
            );

    @Override
    public void processBroadcastElement(
            TrackStatusEvent statusEvent,
            KeyedBroadcastProcessFunction<String, RivalInfoAlert, TrackStatusEvent, PitWindowAlert>.Context ctx,
            Collector<PitWindowAlert> out) throws Exception {
        ctx.getBroadcastState(TRACK_STATUS_STATE).put("current", statusEvent.getStatus());
    }

    @Override
    public void processElement(
            RivalInfoAlert rival,
            KeyedBroadcastProcessFunction<String, RivalInfoAlert, TrackStatusEvent, PitWindowAlert>.ReadOnlyContext ctx,
            Collector<PitWindowAlert> out) throws Exception {

        // no car behind means the driver is last in the tracked group
        Double gapBehind = rival.getGapBehind();
        if (gapBehind == null) {
            return;
        }

        String status = ctx.getBroadcastState(TRACK_STATUS_STATE).get("current");
        if (status == null) {
            status = "1"; // default: green
        }

        double threshold;
        switch (status) {
            case "1" -> threshold = rival.getPitLoss() != null ? rival.getPitLoss() : DEFAULT_GREEN;
            case "6", "7" -> threshold = rival.getVscPitLoss() != null ? rival.getVscPitLoss() : DEFAULT_VSC;
            case "4" -> threshold = rival.getScPitLoss() != null ? rival.getScPitLoss() : DEFAULT_SC;
            default -> {
                return; // yellow ("2"), red ("5"): suppress, pit lane may be closed
            }
        }

        if (gapBehind >= threshold) {
            out.collect(new PitWindowAlert(
                    rival.getDriver(),
                    rival.getLapNumber(),
                    gapBehind,
                    status,
                    threshold
            ));
        }
    }
}
