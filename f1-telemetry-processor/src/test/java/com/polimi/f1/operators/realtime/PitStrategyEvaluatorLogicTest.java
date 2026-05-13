package com.polimi.f1.operators.realtime;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.output.PitSuggestionAlert.SuggestionLabel;

// regression tests for private strategic gates added to improve h=2 timing alignment.
// these checks keep the behavior stable without requiring full flink harness setup.
class PitStrategyEvaluatorLogicTest {

    @Test
    void rivalPitBoost_whenAheadRivalPitsSameLap_boostsUrgency() throws Exception {
        LapEvent ahead = lap("LEC", 20, 1, 100.0, "1");
        LapEvent current = lap("VER", 20, 2, null, "1");
        LapEvent behind = lap("RUS", 20, 3, null, "1");

        double boost = invokeRivalPitBoost(current, List.of(ahead, current, behind), 1);
        assertEquals(10.0, boost, 1e-9);
    }

    @Test
    void rivalPitBoost_capsWhenBothAdjacentCarsPit() throws Exception {
        LapEvent ahead = lap("LEC", 20, 1, 100.0, "1");
        LapEvent current = lap("VER", 20, 2, null, "1");
        LapEvent behind = lap("RUS", 20, 3, 102.0, "1");

        double boost = invokeRivalPitBoost(current, List.of(ahead, current, behind), 1);
        assertEquals(12.0, boost, 1e-9);
    }

    @Test
    void rivalPitBoost_isDisabledOutsideGreenContext() throws Exception {
        LapEvent ahead = lap("LEC", 20, 1, 100.0, "4");
        LapEvent current = lap("VER", 20, 2, null, "4");
        LapEvent behind = lap("RUS", 20, 3, null, "4");

        double boost = invokeRivalPitBoost(current, List.of(ahead, current, behind), 1);
        assertEquals(0.0, boost, 1e-9);
    }

    @Test
    void earlyActionabilityGate_downgradesWeakEarlyActionableCall() throws Exception {
        SuggestionLabel label = invokeEarlyActionabilityGate(
                SuggestionLabel.GOOD_PIT,
                0,
                8.0,
                0.0,
                10.0,
            0.0,
            0.0,
            0.0);

        assertEquals(SuggestionLabel.MONITOR, label);
    }

    @Test
    void earlyActionabilityGate_preservesActionableOnRivalPitTrigger() throws Exception {
        SuggestionLabel label = invokeEarlyActionabilityGate(
                SuggestionLabel.PIT_NOW,
                0,
                8.0,
                0.0,
                10.0,
            10.0,
            0.0,
            0.0);

        assertEquals(SuggestionLabel.PIT_NOW, label);
    }

    @Test
    void earlyActionabilityGate_preservesActionableUnderCaution() throws Exception {
        SuggestionLabel label = invokeEarlyActionabilityGate(
                SuggestionLabel.GOOD_PIT,
                60,
                8.0,
                0.0,
                10.0,
                0.0,
                0.0,
                0.0);

        assertEquals(SuggestionLabel.GOOD_PIT, label);
    }

    @Test
    void earlyActionabilityGate_preservesActionableWhenTimingPressureIsHigh() throws Exception {
        SuggestionLabel label = invokeEarlyActionabilityGate(
                SuggestionLabel.GOOD_PIT,
                0,
                13.0,
                -2.0,
                16.0,
                0.0,
                4.2,
                0.0004);

        assertEquals(SuggestionLabel.GOOD_PIT, label);
    }

    @Test
    void earlyActionabilityGate_downgradesHighPaceWithoutDecisionWindowSignals() throws Exception {
        SuggestionLabel label = invokeEarlyActionabilityGate(
                SuggestionLabel.PIT_NOW,
                0,
                9.0,
                -1.0,
                20.0,
                0.0,
                2.0,
                0.0005);

        assertEquals(SuggestionLabel.MONITOR, label);
    }

    @Test
    void timingGate_downgradesPitNowToGoodPitUnderWeakCautionEvidence() throws Exception {
        Object decision = invokeTimingGate(
                SuggestionLabel.PIT_NOW,
                100.0,
                10.0,
                12,
                "MEDIUM",
                30,
                50,
                "4",
                true,
                2.0);

        assertEquals(SuggestionLabel.GOOD_PIT, readDecisionField(decision, "legacyLabel"));
        assertEquals("OPPORTUNITY", readDecisionField(decision, "semanticLabel").toString());
        assertEquals("CAUTION_TIMING_WEAK", readDecisionField(decision, "timingGateReason"));
        assertFalse((boolean) readDecisionField(decision, "timingGatePassed"));
    }

    @Test
    void timingGate_cautionHardFloorDoesNotBypassWeakTimingCombo() throws Exception {
        Object decision = invokeTimingGate(
                SuggestionLabel.PIT_NOW,
                99.0,
                15.0,
                16,
                "MEDIUM",
                24,
                52,
                "6",
                true,
                3.0);

        assertEquals(SuggestionLabel.GOOD_PIT, readDecisionField(decision, "legacyLabel"));
        assertEquals("CAUTION_GUARD_DOWNGRADE", readDecisionField(decision, "finalDecisionReason"));
    }

    @Test
    void timingGate_reportsTimingPressureUnavailableWhenMissingAndNeeded() throws Exception {
        Object decision = invokeTimingGate(
                SuggestionLabel.PIT_NOW,
                84.0,
                15.0,
                20,
                "MEDIUM",
                20,
                58,
                "1",
                false,
                0.0);

        assertEquals(SuggestionLabel.GOOD_PIT, readDecisionField(decision, "legacyLabel"));
        assertEquals("TIMING_PRESSURE_UNAVAILABLE", readDecisionField(decision, "timingGateReason"));
    }

    @Test
    void timingGate_setsUnknownPhaseWhenRaceProgressCannotBeDerived() throws Exception {
        Object decision = invokeTimingGate(
                SuggestionLabel.PIT_NOW,
                88.0,
                25.0,
                15,
                "MEDIUM",
                20,
                0,
                "1",
                false,
                0.0);

        assertEquals(SuggestionLabel.PIT_NOW, readDecisionField(decision, "legacyLabel"));
        assertEquals("UNKNOWN", readDecisionField(decision, "pitWindowPhase").toString());
        assertTrue((boolean) readDecisionField(decision, "timingGatePassed"));
    }

    @Test
    void priorPromotionDecision_whenPriorsUnavailable_reportsFallbackReason() throws Exception {
        Object timingDecision = invokeTimingGate(
                SuggestionLabel.GOOD_PIT,
                70.0,
                18.0,
                18,
                "MEDIUM",
                25,
                57,
                "1",
                true,
                4.0);
        LapEvent event = lap("VER", 25, 2, null, "1");
        event.setRace("2025 :: Bahrain Grand Prix");
        event.setTotalLaps(57);
        event.setStint(1);
        event.setCompound("MEDIUM");
        event.setTyreLife(18);

        Object priorDecision = invokePriorPromotionDecision(
                true,
                true,
                false,
                new HashMap<>(),
                timingDecision,
                event,
                "1",
                18.0,
                70.0,
                true,
                4.0);

        assertFalse((boolean) readDecisionField(priorDecision, "priorPromotionApplied"));
        assertEquals("PRIORS_UNAVAILABLE", readDecisionField(priorDecision, "priorPromotionReason"));
    }

    @Test
    void priorPromotionDecision_greenOnlySkipsCautionWithExplicitReason() throws Exception {
        Object timingDecision = invokeTimingGate(
                SuggestionLabel.GOOD_PIT,
                70.0,
                25.0,
                20,
                "MEDIUM",
                25,
                57,
                "4",
                true,
                7.0);
        LapEvent event = lap("VER", 25, 2, null, "4");
        event.setRace("2025 :: Bahrain Grand Prix");
        event.setTotalLaps(57);
        event.setStint(1);
        event.setCompound("MEDIUM");
        event.setTyreLife(20);

        Object priorDecision = invokePriorPromotionDecision(
                true,
                true,
                true,
                new HashMap<>(),
                timingDecision,
                event,
                "4",
                25.0,
                70.0,
                true,
                7.0);

        assertFalse((boolean) readDecisionField(priorDecision, "priorPromotionApplied"));
        assertEquals("CAUTION_SKIPPED", readDecisionField(priorDecision, "priorPromotionReason"));
        assertEquals(
                "CAUTION_DISABLED_FOR_C4A_V1",
                readDecisionField(priorDecision, "priorPromotionSkippedReason"));
    }

    @Test
    void c6RivalFilter_rejectsRecencyAboveOneLap() throws Exception {
        Object decision = invokeC6RivalFilter(
                1,
                10.0,
                10.0,
                true,
                false,
                2,
                -1,
                -1,
                18.0,
                true,
                11.0,
                2.4,
                3.0,
                0.9);

        assertFalse((boolean) readDecisionField(decision, "pass"));
        assertEquals("C6_REJECT_RECENCY", readDecisionField(decision, "reason"));
    }

    @Test
    void c6RivalFilter_rejectsUrgencyBelowFloor() throws Exception {
        Object decision = invokeC6RivalFilter(
                1,
                10.0,
                10.0,
                true,
                false,
                1,
                -1,
                -1,
                9.5,
                true,
                12.0,
                2.0,
                2.8,
                0.9);

        assertFalse((boolean) readDecisionField(decision, "pass"));
        assertEquals("C6_REJECT_URGENCY", readDecisionField(decision, "reason"));
    }

    @Test
    void c6RivalFilter_rejectsTimingPressureBelowFloor() throws Exception {
        Object decision = invokeC6RivalFilter(
                1,
                10.0,
                10.0,
                true,
                false,
                1,
                -1,
                -1,
                15.0,
                true,
                9.0,
                2.0,
                2.8,
                0.9);

        assertFalse((boolean) readDecisionField(decision, "pass"));
        assertEquals("C6_REJECT_TIMING_PRESSURE", readDecisionField(decision, "reason"));
    }

    @Test
    void c6RivalFilter_appliesUltraCloseGuardAndRejectsWithoutExtraEvidence() throws Exception {
        Object decision = invokeC6RivalFilter(
                1,
                10.0,
                10.0,
                true,
                false,
                0,
                -1,
                -1,
                25.0,
                true,
                12.0,
                1.2,
                2.0,
                1.0);

        assertFalse((boolean) readDecisionField(decision, "pass"));
        assertEquals("C6_REJECT_ULTRA_CLOSE_GUARD", readDecisionField(decision, "reason"));
        assertTrue((boolean) readDecisionField(decision, "ultraCloseGuardApplied"));
    }

    @Test
    void c6RivalFilter_acceptsValidCfg120Promotion() throws Exception {
        Object decision = invokeC6RivalFilter(
                1,
                10.0,
                10.0,
                true,
                false,
                0,
                -1,
                -1,
                16.0,
                true,
                10.5,
                1.1,
                2.0,
                0.8);

        assertTrue((boolean) readDecisionField(decision, "pass"));
        assertEquals("C6_PROFILE_PASS", readDecisionField(decision, "reason"));
        assertTrue((boolean) readDecisionField(decision, "ultraCloseGuardApplied"));
    }

    private static LapEvent lap(String driver, int lapNumber, int position, Double pitInTime, String trackStatus) {
        LapEvent event = new LapEvent();
        event.setDriver(driver);
        event.setLapNumber(lapNumber);
        event.setPosition(position);
        event.setPitInTime(pitInTime);
        event.setTrackStatus(trackStatus);
        return event;
    }

    private static double invokeRivalPitBoost(LapEvent current, List<LapEvent> laps, int posIndex) throws Exception {
        Method method = PitStrategyEvaluator.class.getDeclaredMethod(
                "computeRivalPitReactionBoost", LapEvent.class, List.class, int.class);
        method.setAccessible(true);
        return (double) method.invoke(null, current, laps, posIndex);
    }

    private static SuggestionLabel invokeEarlyActionabilityGate(
            SuggestionLabel label,
            int trackStatusScore,
            double urgencyScore,
            double strategyPenalty,
            double paceScore,
            double rivalPitReactionBoost,
            double timingPressureScore,
            double paceRatioDelta) throws Exception {
        Method method = PitStrategyEvaluator.class.getDeclaredMethod(
                "applyEarlyActionabilityGate",
                SuggestionLabel.class,
                int.class,
                double.class,
                double.class,
                double.class,
                double.class,
                double.class,
                double.class);
        method.setAccessible(true);
        return (SuggestionLabel) method.invoke(
                null,
                label,
                trackStatusScore,
                urgencyScore,
                strategyPenalty,
                paceScore,
                rivalPitReactionBoost,
                timingPressureScore,
                paceRatioDelta);
    }

    private static Object invokeTimingGate(
            SuggestionLabel label,
            double totalScore,
            double urgencyScore,
            int tyreLife,
            String compound,
            int lapNumber,
            int totalLaps,
            String trackStatus,
            boolean timingPressureAvailable,
            double timingPressureScore) throws Exception {
        Method method = PitStrategyEvaluator.class.getDeclaredMethod(
                "applyDeterministicTimingGate",
                SuggestionLabel.class,
                double.class,
                double.class,
                int.class,
                String.class,
                int.class,
                int.class,
                String.class,
                boolean.class,
                double.class);
        method.setAccessible(true);
        return method.invoke(
                null,
                label,
                totalScore,
                urgencyScore,
                tyreLife,
                compound,
                lapNumber,
                totalLaps,
                trackStatus,
                timingPressureAvailable,
                timingPressureScore);
    }

    private static Object readDecisionField(Object decision, String fieldName) throws Exception {
        var field = decision.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(decision);
    }

    private static Object invokePriorPromotionDecision(
            boolean priorPromotionEnabled,
            boolean priorPromotionStrictMode,
            boolean priorsAvailable,
            Map<String, Object> priors,
            Object timingDecision,
            LapEvent event,
            String trackStatus,
            double urgencyScore,
            double totalScore,
            boolean timingPressureAvailable,
            double timingPressureScore) throws Exception {
        Method method = PitStrategyEvaluator.class.getDeclaredMethod(
                "evaluatePriorPromotionDecision",
                boolean.class,
                boolean.class,
                boolean.class,
                Map.class,
                timingDecision.getClass(),
                LapEvent.class,
                String.class,
                double.class,
                double.class,
                boolean.class,
                double.class);
        method.setAccessible(true);
        return method.invoke(
                null,
                priorPromotionEnabled,
                priorPromotionStrictMode,
                priorsAvailable,
                priors,
                timingDecision,
                event,
                trackStatus,
                urgencyScore,
                totalScore,
                timingPressureAvailable,
                timingPressureScore);
    }

    private static Object invokeC6RivalFilter(
            int rivalRecentMaxLaps,
            double rivalMinUrgency,
            double rivalMinTimingPressure,
            boolean rivalUltraCloseGapGuardEnabled,
            boolean cautionRegime,
            int aheadRecent,
            int behindRecent,
            int teammateRecent,
            double urgencyScore,
            boolean timingPressureAvailable,
            double timingPressureScore,
            Double gapAhead,
            Double gapBehind,
            double tireLifeRatio) throws Exception {
        Method method = PitStrategyEvaluator.class.getDeclaredMethod(
                "evaluateC6TunedRivalFilter",
                int.class,
                double.class,
                double.class,
                boolean.class,
                boolean.class,
                int.class,
                int.class,
                int.class,
                double.class,
                boolean.class,
                double.class,
                Double.class,
                Double.class,
                double.class);
        method.setAccessible(true);
        return method.invoke(
                null,
                rivalRecentMaxLaps,
                rivalMinUrgency,
                rivalMinTimingPressure,
                rivalUltraCloseGapGuardEnabled,
                cautionRegime,
                aheadRecent,
                behindRecent,
                teammateRecent,
                urgencyScore,
                timingPressureAvailable,
                timingPressureScore,
                gapAhead,
                gapBehind,
                tireLifeRatio);
    }
}
