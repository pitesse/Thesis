package com.polimi.f1.operators.realtime;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
