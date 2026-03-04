package com.polimi.f1.alerts;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.polimi.f1.events.TrackStatusEvent;
import com.polimi.f1.model.PitWindowAlert;
import com.polimi.f1.model.RivalInfoAlert;

// evaluates whether a driver can pit without losing position based on gap to car behind.
// the threshold adapts dynamically based on track status:
//   green (status "1"): pitLoss value (default 25s), typical pit stop time loss at most circuits
//   vsc   (status "6"): pitLoss - 10s (min 5s), pit lane delta is reduced under vsc
//   sc    (status "4"): pitLoss - 20s (min 5s), field bunches up, minimal pit stop loss
//   yellow/red:          suppressed — pit lane may be closed or unsafe
//
// the green threshold is configurable via --pit-loss to adapt to different circuits,
// ex: monaco ~20s, monza ~25s, spa ~22s. vsc and sc scale relative to the green value.
// uses broadcast state to receive track status updates (same pattern as TrackStatusEnricher).
public class PitWindowDetector
        extends KeyedBroadcastProcessFunction<String, RivalInfoAlert, TrackStatusEvent, PitWindowAlert> {

    // thresholds (seconds), green is set via constructor, vsc/sc scale relative to it
    private final double greenThreshold;
    private final double vscThreshold;
    private final double scThreshold;

    public PitWindowDetector(double pitLoss) {
        this.greenThreshold = pitLoss;
        this.vscThreshold = Math.max(pitLoss - 10.0, 5.0); // TODO maybe make this tie to the specific track's vsc delta if available?
        this.scThreshold = Math.max(pitLoss - 20.0, 5.0); // TODO same as above but for safty
    } 

    public PitWindowDetector() {
        this(25.0); // TODO maybe better to use hardcoded Pit time for each track instead of a generic default? 25s is a common average but can vary significantly
    }

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
            case "1" -> threshold = greenThreshold;
            case "6" -> threshold = vscThreshold; 
            case "7" -> threshold = vscThreshold;
            case "4" -> threshold = scThreshold;
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
