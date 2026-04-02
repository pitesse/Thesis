package com.polimi.f1.operators.groundtruth;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.output.PitStopEvaluationAlert;
import com.polimi.f1.model.output.PitStopEvaluationAlert.Result;

// tests the state-machine-based pit cycle evaluation using flink's test harness.
// the evaluator tracks rival clusters (car ahead + behind), detects offset strategies,
// and normalizes gap deltas as % of baseline lap time for 8-label classification.
//
// keyed by constant "RACE" (global visibility). gap sign convention:
//   computeDirectedGap(A, B) -> positive means A is behind B, negative means A is ahead.
//   ex: VER at P2, LEC at P1, gapToCarAhead=2.5 -> computeDirectedGap(VER, LEC) = +2.5
//   ex: VER at P1, LEC at P2, gapToCarAhead=1.0 -> computeDirectedGap(VER, LEC) = -1.0
//
// gapDeltaPct = (postGap - preGap) / baseline * 100
//   negative delta = gap improved (undercut/overcut), positive delta = gap worsened
class PitStopEvaluatorTest {

    private KeyedOneInputStreamOperatorTestHarness<String, LapEvent, PitStopEvaluationAlert> harness;

    private static final long T0 = 1_000_000_000_000L;

    @BeforeEach
    void setup() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new PitStopEvaluator()),
                e -> "RACE",
                Types.STRING
        );
        harness.open();
    }

    @AfterEach
    void teardown() throws Exception {
        harness.close();
    }

    // VER (P2, 2.5s behind LEC) pits lap 20. LEC pits on lap 25.
    // after both settle (lap 29), VER ahead of LEC (P1, LEC P2 gap 1.0s).
    // pre-gap = +2.5, post-gap = -1.0 -> delta = -3.5s / 82.0 = -4.3% -> SUCCESS_UNDERCUT
    @Test
    void undercutSuccess_driverPitsFirst() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        // lap 20: VER (P2, gap 2.5s) pits, LEC (P1) stays out
        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), ts(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.5, "1", 51, 1), ts(20));

        // laps 21-24: VER settles on new tires, stays P2 behind LEC
        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "1", 51, 1), ts(l));
            process(makeLap("VER", l, 2, "HARD", l - 20, null, null, 1.5, "1", 51, 2), ts(l));
        }

        // lap 25: LEC pits (rival pitted after driver -> undercut scenario)
        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), ts(25));
        process(makeLap("VER", 25, 1, "HARD", 5, null, null, null, "1", 51, 2), ts(25));

        // laps 26-29: LEC settles, VER now P1, LEC P2. gap 1.0s to VER.
        for (int l = 26; l <= 29; l++) {
            process(makeLap("VER", l, 1, "HARD", 5 + (l - 25), null, null, null, "1", 51, 2), ts(l));
            process(makeLap("LEC", l, 2, "HARD", l - 25, null, null, 1.0, "1", 51, 2), ts(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.SUCCESS_UNDERCUT, verEval.getResult());
        assertEquals("LEC", verEval.getRivalAhead());
        assertTrue(verEval.isDriverPittedFirst(), "VER pitted first");
        assertFalse(verEval.isOffsetStrategy());
        assertEquals("RIVAL_PIT", verEval.getResolvedVia());
        assertNotNull(verEval.getGapDeltaPct());
        assertTrue(verEval.getGapDeltaPct() < -0.5, "gap delta should be strongly negative (undercut gain)");
    }

    // LEC (P1) pits first on lap 20, VER (P2) stays out and pits later on lap 25.
    // by staying out, VER gained track position. after VER pits and settles, VER P1, LEC P2.
    // pre-gap at VER's pit: VER P1, LEC P2 gap 3.0 -> computeDirectedGap(VER,LEC) = -3.0
    // post-gap after settle: VER P1, LEC P2 gap 4.0 -> computeDirectedGap(VER,LEC) = -4.0
    // delta = (-4.0 - (-3.0)) / 82 * 100 = -1.22% -> gained, driverPittedFirst=false -> OVERCUT
    @Test
    void overcutSuccess_rivalPitsFirst() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        // lap 20: LEC (P1) pits, VER (P2) stays out
        process(makeLapWithPit("LEC", 20, 1, "SOFT", 19, 100.0, null, null, "1", 51, 1), ts(20));
        process(makeLap("VER", 20, 2, "SOFT", 19, null, null, 2.0, "1", 51, 1), ts(20));

        // laps 21-24: VER inherits P1, LEC drops to P2 with out-lap then settles at 3.0s gap
        for (int l = 21; l <= 24; l++) {
            process(makeLap("VER", l, 1, "SOFT", 19 + (l - 20), null, null, null, "1", 51, 1), ts(l));
            process(makeLap("LEC", l, 2, "HARD", l - 20, null, null, 3.0, "1", 51, 2), ts(l));
        }

        // lap 25: VER pits (driver pitted after rival). VER P1, LEC P2 gap 3.0
        process(makeLap("LEC", 25, 2, "HARD", 5, null, null, 3.0, "1", 51, 2), ts(25));
        process(makeLapWithPit("VER", 25, 1, "SOFT", 24, 200.0, null, null, "1", 51, 1), ts(25));

        // laps 26-29: VER settles. VER at P1, LEC at P2, gap grows to 4.0 (overcut gain)
        for (int l = 26; l <= 29; l++) {
            process(makeLap("VER", l, 1, "HARD", l - 25, null, null, null, "1", 51, 2), ts(l));
            process(makeLap("LEC", l, 2, "HARD", 5 + (l - 25), null, null, 4.0, "1", 51, 2), ts(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.SUCCESS_OVERCUT, verEval.getResult());
        assertFalse(verEval.isDriverPittedFirst(), "VER pitted second (overcut)");
    }

    // VER (P2, 2.0s behind LEC) pits. after settling, gap worsens.
    // LEC pits on lap 25, both settle. VER stays P2, gap widens to 5.0s.
    // pre-gap = +2.0, post-gap = +5.0 -> delta = +3.0/82*100 = +3.7% -> FAILURE_PACE_DEFICIT
    @Test
    void failurePaceDeficit_gapWorsens() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), ts(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.0, "1", 51, 1), ts(20));

        // out-lap + settling, VER stays P2, gap widens
        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "1", 51, 1), ts(l));
            process(makeLap("VER", l, 2, "HARD", l - 20, null, null, 6.0, "1", 51, 2), ts(l));
        }

        // LEC also pits on lap 25 to resolve the cycle
        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), ts(25));
        process(makeLap("VER", 25, 1, "HARD", 5, null, null, null, "1", 51, 2), ts(25));

        // after both settle: LEC P1, VER P2, gap 5.0s (worsened from 2.0s)
        for (int l = 26; l <= 29; l++) {
            process(makeLap("LEC", l, 1, "HARD", l - 25, null, null, null, "1", 51, 2), ts(l));
            process(makeLap("VER", l, 2, "HARD", 5 + (l - 25), null, null, 5.0, "1", 51, 2), ts(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.FAILURE_PACE_DEFICIT, verEval.getResult());
    }

    // VER pits under safety car with minimal gap change -> SUCCESS_FREE_STOP.
    // pre-gap = +2.0, post-gap = +2.3 -> delta = +0.3/82*100 = +0.37% -> within 1% band under SC
    @Test
    void freeStop_pitUnderSafetyCar() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        // lap 20: SC deploys, VER pits
        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "4", 51, 1), ts(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.0, "4", 51, 1), ts(20));

        // under SC, VER stays P2, gaps compress
        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "4", 51, 1), ts(l));
            process(makeLap("VER", l, 2, "HARD", l - 20, null, null, 2.3, "4", 51, 2), ts(l));
        }

        // LEC also pits under SC on lap 25 to resolve
        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "4", 51, 1), ts(25));
        process(makeLap("VER", 25, 1, "HARD", 5, null, null, null, "4", 51, 2), ts(25));

        // after both settle under green: LEC P1, VER P2, gap barely changed (2.3s)
        for (int l = 26; l <= 29; l++) {
            process(makeLap("LEC", l, 1, "HARD", l - 25, null, null, null, "1", 51, 2), ts(l));
            process(makeLap("VER", l, 2, "HARD", 5 + (l - 25), null, null, 2.3, "1", 51, 2), ts(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.SUCCESS_FREE_STOP, verEval.getResult());
        assertEquals("4", verEval.getTrackStatusAtPit());
    }

    // VER pits on lap 10, rival LEC never pits (1-stop strategy).
    // offset timeout triggers at 40% of remaining laps. gap improved -> OFFSET_ADVANTAGE
    @Test
    void offsetAdvantage_rivalNeverPits() throws Exception {
        int totalLaps = 50;
        feedGreenLapsTotal("VER", "LEC", 5, 9, 82.0, 81.5, totalLaps);

        // lap 10: VER pits, LEC stays out. remaining = 40 laps, timeout at 40% = 16 -> lap 26
        process(makeLap("LEC", 10, 1, "HARD", 10, null, null, null, "1", totalLaps, 1), ts(10));
        process(makeLapWithPit("VER", 10, 2, "SOFT", 9, 100.0, null, 3.0, "1", totalLaps, 1), ts(10));

        // out-lap + settle laps
        for (int l = 11; l <= 14; l++) {
            process(makeLap("LEC", l, 1, "HARD", l, null, null, null, "1", totalLaps, 1), ts(l));
            process(makeLap("VER", l, 2, "HARD", l - 10, null, null, 5.0, "1", totalLaps, 2), ts(l));
        }

        // laps 15-26: VER on fresh tires closing the gap, stays P2
        for (int l = 15; l <= 26; l++) {
            double gap = Math.max(1.0, 5.0 - (l - 14) * 0.3);
            process(makeLap("LEC", l, 1, "HARD", l, null, null, null, "1", totalLaps, 1), ts(l));
            process(makeLap("VER", l, 2, "HARD", l - 10, null, null, gap, "1", totalLaps, 2), ts(l));
        }

        // at lap 26 (offset timeout), gap is ~1.4s (improved from 3.0)
        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist after offset timeout");
        assertEquals(Result.OFFSET_ADVANTAGE, verEval.getResult());
        assertTrue(verEval.isOffsetStrategy());
        assertEquals("OFFSET_TIMEOUT", verEval.getResolvedVia());
    }

    // no pit entry events -> no evaluations emitted
    @Test
    void noPitEntry_noOutput() throws Exception {
        process(makeLap("VER", 10, 1, "SOFT", 10, null, null, null, "1", 51, 1), ts(10));
        process(makeLap("VER", 11, 1, "SOFT", 11, null, null, null, "1", 51, 1), ts(11));
        process(makeLap("VER", 12, 1, "SOFT", 12, null, null, null, "1", 51, 1), ts(12));

        assertTrue(extractResults().isEmpty());
    }

    // VER pits, LEC pits later. after both settle, VER still P2 with similar gap.
    // pre-gap = +2.0, post-gap = +2.2 -> delta = +0.2 / 82.0 * 100 = +0.24% -> SUCCESS_DEFEND
    @Test
    void successDefend_gapMaintained() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), ts(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.0, "1", 51, 1), ts(20));

        // VER stays P2, gap roughly same
        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "1", 51, 1), ts(l));
            process(makeLap("VER", l, 2, "HARD", l - 20, null, null, 2.2, "1", 51, 2), ts(l));
        }

        // LEC also pits to resolve
        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), ts(25));
        process(makeLap("VER", 25, 1, "HARD", 5, null, null, null, "1", 51, 2), ts(25));

        // after both settle: LEC P1, VER P2, gap 2.2 (barely changed from 2.0)
        for (int l = 26; l <= 29; l++) {
            process(makeLap("LEC", l, 1, "HARD", l - 25, null, null, null, "1", 51, 2), ts(l));
            process(makeLap("VER", l, 2, "HARD", 5 + (l - 25), null, null, 2.2, "1", 51, 2), ts(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.SUCCESS_DEFEND, verEval.getResult());
    }

    // dual rival tracking: verify both rivalAhead and rivalBehind captured.
    // SAI (P3) is processed AFTER VER's pit event on lap 20, so the operator
    // must fall back to lap 19 data to find the rival behind.
    @Test
    void rivalCluster_capturesBothRivals() throws Exception {
        feedGreenLaps3("LEC", "VER", "SAI", 15, 19, 81.0, 82.0, 82.5);

        // lap 20: LEC first, then VER pits, then SAI arrives
        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), ts(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.5, "1", 51, 1), ts(20));
        process(makeLap("SAI", 20, 3, "MEDIUM", 10, null, null, 1.5, "1", 51, 1), ts(20));

        // settle + rival resolution: VER drops to P3
        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "1", 51, 1), ts(l));
            process(makeLap("SAI", l, 2, "MEDIUM", 10 + (l - 20), null, null, 4.0, "1", 51, 1), ts(l));
            process(makeLap("VER", l, 3, "HARD", l - 20, null, null, 2.0, "1", 51, 2), ts(l));
        }

        // LEC pits to resolve VER's cycle
        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), ts(25));
        process(makeLap("SAI", 25, 1, "MEDIUM", 15, null, null, null, "1", 51, 1), ts(25));
        process(makeLap("VER", 25, 2, "HARD", 5, null, null, 2.0, "1", 51, 2), ts(25));

        // after settle: SAI P1, VER P2, LEC P3
        for (int l = 26; l <= 29; l++) {
            process(makeLap("SAI", l, 1, "MEDIUM", 15 + (l - 25), null, null, null, "1", 51, 1), ts(l));
            process(makeLap("VER", l, 2, "HARD", 5 + (l - 25), null, null, 1.5, "1", 51, 2), ts(l));
            process(makeLap("LEC", l, 3, "HARD", l - 25, null, null, 3.0, "1", 51, 2), ts(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals("LEC", verEval.getRivalAhead());
        assertEquals("SAI", verEval.getRivalBehind());
        assertNotNull(verEval.getPrePitGapAhead());
        assertNotNull(verEval.getPrePitGapBehind());
    }

    // warmup-lap pit entries are explicitly separated from strategic labels.
    @Test
    void warmupPit_emitsMissingPreGapWithEarlyLapFilter() throws Exception {
        process(makeLapWithPit("VER", 2, 2, "SOFT", 1, 20.0, null, 1.0, "1", 51, 1), ts(2));

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "warmup pit should emit unresolved record");
        assertEquals(Result.UNRESOLVED_MISSING_PRE_GAP, verEval.getResult());
        assertEquals("EARLY_LAP_FILTER", verEval.getResolvedVia());
    }

    // extreme gap movements are filtered into incident-like unresolved labels.
    @Test
    void incidentLikeGapDelta_emitsUnresolvedIncidentFilter() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), ts(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.0, "1", 51, 1), ts(20));

        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "1", 51, 1), ts(l));
            process(makeLap("VER", l, 2, "HARD", l - 20, null, null, 10.0, "1", 51, 2), ts(l));
        }

        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), ts(25));
        process(makeLap("VER", 25, 1, "HARD", 5, null, null, null, "1", 51, 2), ts(25));

        for (int l = 26; l <= 29; l++) {
            process(makeLap("LEC", l, 1, "HARD", l - 25, null, null, null, "1", 51, 2), ts(l));
            process(makeLap("VER", l, 2, "HARD", 5 + (l - 25), null, null, 20.0, "1", 51, 2), ts(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.UNRESOLVED_INCIDENT_FILTER, verEval.getResult());
        assertEquals("RIVAL_PIT", verEval.getResolvedVia());
    }

    // when post-gap cannot be reconstructed, evaluator can salvage with pace-shift
    // if both drivers have enough clean laps around pre and post anchors.
    @Test
    void rivalPitPaceShiftFallback_whenPostGapMissing_emitsStrategicLabel() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), tsFast(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.0, "1", 51, 1), tsFast(20));

        for (int l = 21; l <= 24; l++) {
            process(makeGreenLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), 81.7, null, 51, 1), tsFast(l));
            process(makeGreenLap("VER", l, 10, "HARD", l - 20, 81.1, 0.8, 51, 2), tsFast(l));
        }

        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), tsFast(25));
        process(makeLap("VER", 25, 10, "HARD", 5, null, null, 0.8, "1", 51, 2), tsFast(25));

        // keep drivers far apart in position so direct gap reconstruction is unavailable.
        for (int l = 26; l <= 32; l++) {
            process(makeGreenLap("LEC", l, 1, "HARD", l - 25, 82.1, null, 51, 2), tsFast(l));
            process(makeGreenLap("VER", l, 10, "HARD", 5 + (l - 25), 80.9, 0.8, 51, 2), tsFast(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.SUCCESS_UNDERCUT, verEval.getResult());
        assertEquals("RIVAL_PIT_PACE_SHIFT", verEval.getResolvedVia());
        assertNotNull(verEval.getGapDeltaPct());
        assertTrue(verEval.getGapDeltaPct() < -0.5, "pace-shift should show clear gain");
    }

    // fallback stays conservative, missing pace evidence must remain unresolved.
    @Test
    void rivalPitPaceShiftFallback_withoutPaceEvidence_staysUnresolved() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), tsFast(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.0, "1", 51, 1), tsFast(20));

        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "1", 51, 1), tsFast(l));
            process(makeLap("VER", l, 10, "HARD", l - 20, null, null, 0.8, "1", 51, 2), tsFast(l));
        }

        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), tsFast(25));
        process(makeLap("VER", 25, 10, "HARD", 5, null, null, 0.8, "1", 51, 2), tsFast(25));

        for (int l = 26; l <= 32; l++) {
            process(makeLap("LEC", l, 1, "HARD", l - 25, null, null, null, "1", 51, 2), tsFast(l));
            process(makeLap("VER", l, 10, "HARD", 5 + (l - 25), null, null, 0.8, "1", 51, 2), tsFast(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.UNRESOLVED_MISSING_POST_GAP, verEval.getResult());
        assertEquals("INSUFFICIENT_DATA", verEval.getResolvedVia());
    }

    // when both directed-gap and pace-shift fail, clear ordinal rival overtake is salvaged.
    @Test
    void rivalPitPositionalFallback_whenPostGapMissingAndOrderFlips_emitsStrategicLabel() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), tsFast(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.0, "1", 51, 1), tsFast(20));

        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "1", 51, 1), tsFast(l));
            process(makeLap("VER", l, 10, "HARD", l - 20, null, null, 0.8, "1", 51, 2), tsFast(l));
        }

        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), tsFast(25));
        process(makeLap("VER", 25, 10, "HARD", 5, null, null, 0.8, "1", 51, 2), tsFast(25));

        // no P5 events are emitted, directed gap cannot be reconstructed at comparison laps.
        for (int l = 26; l <= 32; l++) {
            process(makeLap("VER", l, 4, "HARD", 5 + (l - 25), null, null, 1.2, "1", 51, 2), tsFast(l));
            process(makeLap("LEC", l, 6, "HARD", l - 25, null, null, 1.1, "1", 51, 2), tsFast(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.SUCCESS_UNDERCUT, verEval.getResult());
        assertEquals("POSITIONAL_FALLBACK", verEval.getResolvedVia());
        assertNull(verEval.getGapDeltaPct(), "positional fallback does not fabricate time delta");
    }

    // positional fallback stays conservative when pre and post rival order is unchanged.
    @Test
    void rivalPitPositionalFallback_whenOrderUnchanged_staysUnresolved() throws Exception {
        feedGreenLaps("VER", "LEC", 15, 19, 82.0, 81.5);

        process(makeLap("LEC", 20, 1, "MEDIUM", 10, null, null, null, "1", 51, 1), tsFast(20));
        process(makeLapWithPit("VER", 20, 2, "SOFT", 19, 100.0, null, 2.0, "1", 51, 1), tsFast(20));

        for (int l = 21; l <= 24; l++) {
            process(makeLap("LEC", l, 1, "MEDIUM", 10 + (l - 20), null, null, null, "1", 51, 1), tsFast(l));
            process(makeLap("VER", l, 8, "HARD", l - 20, null, null, 0.8, "1", 51, 2), tsFast(l));
        }

        process(makeLapWithPit("LEC", 25, 1, "MEDIUM", 15, 200.0, null, null, "1", 51, 1), tsFast(25));
        process(makeLap("VER", 25, 8, "HARD", 5, null, null, 0.8, "1", 51, 2), tsFast(25));

        // driver stays behind rival, and missing gapToCarAhead on VER keeps directed gap unavailable.
        for (int l = 26; l <= 32; l++) {
            process(makeLap("LEC", l, 4, "HARD", l - 25, null, null, 1.2, "1", 51, 2), tsFast(l));
            process(makeLap("VER", l, 5, "HARD", 5 + (l - 25), null, null, null, "1", 51, 2), tsFast(l));
        }

        List<PitStopEvaluationAlert> results = extractResults();
        PitStopEvaluationAlert verEval = findByDriver(results, "VER");
        assertNotNull(verEval, "VER evaluation should exist");
        assertEquals(Result.UNRESOLVED_MISSING_POST_GAP, verEval.getResult());
        assertEquals("INSUFFICIENT_DATA", verEval.getResolvedVia());
    }

    // --- helpers ---
    private static long ts(int lap) {
        return T0 + (lap * 90_000L);
    }

    private static long tsFast(int lap) {
        return T0 + (lap * 60_000L);
    }

    private void process(LapEvent event, long timestamp) throws Exception {
        event.setEventTimeMillis(timestamp);
        harness.processElement(new StreamRecord<>(event, timestamp));
    }

    // creates a standard lap event (no pit). stint parameter allows tracking stint changes.
    private static LapEvent makeLap(String driver, int lapNum, int position,
            String compound, int tyreLife, Double pitIn, Double pitOut,
            Double gapToCarAhead, String trackStatus, int totalLaps, int stint) {
        LapEvent lap = new LapEvent();
        lap.setDriver(driver);
        lap.setLapNumber(lapNum);
        lap.setPosition(position);
        lap.setCompound(compound);
        lap.setPitInTime(pitIn);
        lap.setPitOutTime(pitOut);
        lap.setTyreLife(tyreLife);
        lap.setRace("Test Grand Prix");
        lap.setTrackStatus(trackStatus);
        lap.setGapToCarAhead(gapToCarAhead);
        lap.setTotalLaps(totalLaps);
        lap.setStint(stint);
        return lap;
    }

    // creates a lap event with pit entry
    private static LapEvent makeLapWithPit(String driver, int lapNum, int position,
            String compound, int tyreLife, Double pitIn, Double pitOut,
            Double gapToCarAhead, String trackStatus, int totalLaps, int stint) {
        return makeLap(driver, lapNum, position, compound, tyreLife,
                pitIn, pitOut, gapToCarAhead, trackStatus, totalLaps, stint);
    }

    // creates a green-flag lap with lap time for stint best tracking
    private static LapEvent makeGreenLap(String driver, int lapNum, int position,
            String compound, int tyreLife, double lapTime, Double gapToCarAhead,
            int totalLaps, int stint) {
        LapEvent lap = makeLap(driver, lapNum, position, compound, tyreLife,
                null, null, gapToCarAhead, "1", totalLaps, stint);
        lap.setLapTime(lapTime);
        return lap;
    }

    // feeds green flag laps for two drivers to build stint bests.
    // d2 at P1 (no gap), d1 at P2 (gap 2.5s)
    private void feedGreenLaps(String d1, String d2, int fromLap, int toLap,
            double d1Time, double d2Time) throws Exception {
        feedGreenLapsTotal(d1, d2, fromLap, toLap, d1Time, d2Time, 51);
    }

    private void feedGreenLapsTotal(String d1, String d2, int fromLap, int toLap,
            double d1Time, double d2Time, int totalLaps) throws Exception {
        for (int l = fromLap; l <= toLap; l++) {
            process(makeGreenLap(d2, l, 1, "MEDIUM", l, d2Time, null, totalLaps, 1), ts(l));
            process(makeGreenLap(d1, l, 2, "SOFT", l, d1Time, 2.5, totalLaps, 1), ts(l));
        }
    }

    private void feedGreenLaps3(String d1, String d2, String d3,
            int fromLap, int toLap, double d1Time, double d2Time, double d3Time)
            throws Exception {
        for (int l = fromLap; l <= toLap; l++) {
            process(makeGreenLap(d1, l, 1, "MEDIUM", l, d1Time, null, 51, 1), ts(l));
            process(makeGreenLap(d2, l, 2, "SOFT", l, d2Time, 2.5, 51, 1), ts(l));
            process(makeGreenLap(d3, l, 3, "MEDIUM", l, d3Time, 1.5, 51, 1), ts(l));
        }
    }

    private PitStopEvaluationAlert findByDriver(List<PitStopEvaluationAlert> results, String driver) {
        return results.stream()
                .filter(r -> driver.equals(r.getDriver()))
                .findFirst()
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private List<PitStopEvaluationAlert> extractResults() {
        List<PitStopEvaluationAlert> results = new ArrayList<>();
        for (Object obj : harness.getOutput()) {
            if (obj instanceof StreamRecord) {
                results.add(((StreamRecord<PitStopEvaluationAlert>) obj).getValue());
            }
        }
        return results;
    }
}
