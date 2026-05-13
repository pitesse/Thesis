package com.polimi.f1.operators.realtime;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.polimi.f1.model.TrackStatusCodes;
import com.polimi.f1.model.input.LapEvent;
import com.polimi.f1.model.input.TrackStatusEvent;
import com.polimi.f1.model.output.PitSuggestionAlert;
import com.polimi.f1.model.output.PitSuggestionAlert.SuggestionLabel;
import com.polimi.f1.state.realtime.DriverPitState;

// computes a continuous fuzzy-logic "pit desirability score" (0.0-100.0) for each driver,
// using fully continuous scoring curves that eliminate the discrete score clumping of the
// previous implementation.
//
// architecture: KeyedBroadcastProcessFunction receiving lap events (keyed by "RACE")
// and broadcast track status changes. strategy is evaluated instantly using a
// continuous O(1) snapshot of the physical grid from latestGridState,
// filtering out retired or ghost cars.
//
// scoring dimensions (all continuous except track status):
//   pace:     0-30, power 1.5 curve: 30 * min(1.0, (paceRatio / 0.03)^1.5)
//   traffic: -30 to +30, linear interpolation based on emergence gap
//   urgency:  0-30, quadratic ramp: 30 * min(1.0, ((tyreRatio - 0.5) / 0.5)^2)
//   strategy: -15 to 0, continuous deficit penalty
//   timing:   0-10, bounded decision-window pressure from pace acceleration and local gap expansion
//   track:    0 or 60 (crisp, binary event, sc/vsc only)
//   eor:     -100 to 0, logistic sigmoid: -100 / (1 + e^(-15*(ratio-0.92)))
//            near-zero until 85%, ramps through 90-95%, effectively -100 at 98%+.
//            under SC at 90%: +60 track overcomes ~-18 eor penalty -> GOOD_PIT.
//            under SC at 95%+: even +60 can't overcome ~-89 eor -> correctly suppressed.
//
// multi-label output: MONITOR (40-59), GOOD_PIT (60-79), PIT_NOW (80+), LOST_CHANCE (peak decay)
//
// emit-gate: suppresses notification spam by tracking per-driver emissions within a stint.
// first alert above threshold fires immediately. subsequent laps only re-emit if the score
// has increased by >= 10 points (escalation) or track status changed (new opportunity).
//
// lost_chance detection: tracks per-driver peak score. if score was >= 70 but drops below 40
// while degradation worsens, emits LOST_CHANCE once per stint.
//
// keyed by constant "RACE" for global position-ladder visibility (same as DropZoneEvaluator).
// max-lap trigger advances global race progress and refreshes snapshot-based evaluation.
public class PitStrategyEvaluator
        extends KeyedBroadcastProcessFunction<String, LapEvent, TrackStatusEvent, PitSuggestionAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(PitStrategyEvaluator.class);

    // broadcast state descriptor, shared with F1StreamingJob for .broadcast() call.
    // same pattern as TrackStatusEnricher but separate instance for this operator.
    public static final MapStateDescriptor<String, String> TRACK_STATUS_STATE
            = new MapStateDescriptor<>(
                    "pit-strategy-track-status",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
            );

    // emit threshold: minimum score to generate an alert
    private static final double EMIT_THRESHOLD = 40.0;
    private static final double GOOD_PIT_THRESHOLD = 60.0;
    private static final double PIT_NOW_THRESHOLD = 80.0;
    private static final double PROMOTED_GOOD_PIT_MIN_SCORE = 52.0;
    private static final double PROMOTED_GOOD_PIT_MIN_URGENCY = 10.0;
    private static final double PROMOTED_GOOD_PIT_MIN_PACE = 8.0;
    private static final double TRAFFIC_BLOCK_THRESHOLD = 0.0;
    private static final double CRITICAL_URGENCY_SCORE = 28.0;
    private static final double RIVAL_PIT_REACTION_BOOST_AHEAD = 10.0;
    private static final double RIVAL_PIT_REACTION_BOOST_BEHIND = 6.0;
    private static final double MAX_RIVAL_PIT_REACTION_BOOST = 12.0;
    private static final int RIVAL_PIT_REACTION_LAP_WINDOW = 1;
    private static final double EARLY_ACTION_MIN_URGENCY = 12.0;
    private static final double EARLY_ACTION_MAX_STRATEGY_PENALTY = -3.0;
    private static final double EARLY_ACTION_MIN_PACE_ESCALATION = 18.0;
    private static final double EARLY_ACTION_MIN_TIMING_PRESSURE = 3.5;
    private static final double EARLY_ACTION_MIN_PACE_ACCELERATION = 0.001;
    private static final int DECISION_EPISODE_HORIZON_LAPS = 2;
    private static final double MAX_CONFIDENCE_PENALTY = 12.0;
    private static final double CONFIDENCE_PENALTY_ACTIONABLE = 6.0;
    private static final double LOW_CONFIDENCE_THRESHOLD = 0.45;
    private static final double MIN_CONFIDENCE_FOR_ACTIONABLE = 0.30;
    private static final double MAX_COMPETITIVE_PRESSURE = 8.0;
    private static final double COMPETITIVE_GAP_REF = 1.8;
    private static final int COMPETITIVE_RIVAL_PIT_WINDOW = 2;
    private static final double WEAK_URGENCY_LT = 19.918;
    private static final double LOW_TYRE_LIFE_LT = 17.000;
    private static final double EARLY_PROGRESS_LT = 0.25;
    private static final double EARLY_TIRE_LIFE_RATIO_LT = 0.85;
    private static final double OVERDUE_TIRE_RATIO_OVERRIDE = 1.20;
    private static final double CAUTION_MIN_TIRE_LIFE_RATIO = 0.90;
    private static final double CAUTION_MIN_TIMING_PRESSURE = 6.0;
    private static final double CAUTION_SCORE_HARD_FLOOR = 95.0;
    private static final double PIT_NOW_MIN_TIRE_LIFE_RATIO = 1.00;
    private static final double PRIOR_PROMOTION_MIN_TIRE_LIFE_RATIO = 0.90;
    private static final double PRIOR_PROMOTION_MIN_TIMING_PRESSURE = 3.5;
    private static final boolean PRIOR_PROMOTION_GREEN_ONLY = true;
    private static final String PRIOR_PROMOTION_CAUTION_SKIP_REASON = "CAUTION_DISABLED_FOR_C4A_V1";

    private static final String PRIORS_ENABLED_SETTING = "PIT_WINDOW_PRIORS_ENABLED";
    private static final String PRIORS_PATH_SETTING = "PIT_WINDOW_PRIORS_JSON";
    private static final String DEFAULT_PRIORS_PATH = "/opt/flink/data_lake/reports/pit_window_priors_2022_2024.json";
    private static final String FLAG_PRIOR_PROMOTION_ENABLED = "SDE_PRIOR_PROMOTION_ENABLED";
    private static final String FLAG_PRIOR_PROMOTION_STRICT_MODE = "SDE_PRIOR_PROMOTION_STRICT_MODE";
    private static final String FLAG_RIVAL_PRESSURE_ENABLED = "SDE_RIVAL_PRESSURE_ENABLED";
    private static final String FLAG_RIVAL_PRESSURE_CAUTION_ENABLED = "SDE_RIVAL_PRESSURE_CAUTION_ENABLED";
    private static final String FLAG_PRIOR_SUPPRESSION_ENABLED = "SDE_PRIOR_SUPPRESSION_ENABLED";
    private static final String FLAG_C6_TUNED_RIVAL_PROFILE_ENABLED = "SDE_C6_TUNED_RIVAL_PROFILE_ENABLED";
    private static final String SETTING_RIVAL_RECENT_MAX_LAPS = "SDE_RIVAL_RECENT_MAX_LAPS";
    private static final String SETTING_RIVAL_MIN_URGENCY = "SDE_RIVAL_MIN_URGENCY";
    private static final String SETTING_RIVAL_MIN_TIMING_PRESSURE = "SDE_RIVAL_MIN_TIMING_PRESSURE";
    private static final String SETTING_RIVAL_ULTRA_CLOSE_GAP_GUARD_ENABLED = "SDE_RIVAL_ULTRA_CLOSE_GAP_GUARD_ENABLED";
    private static final int PRIOR_MIN_SAMPLES_RACE_COMPOUND_STINT = 8;
    private static final int PRIOR_MIN_SAMPLES_RACE_STINT = 8;
    private static final int PRIOR_MIN_SAMPLES_COMPOUND_STINT = 12;
    private static final int PRIOR_MIN_SAMPLES_GLOBAL_STINT = 20;
    private static final double PRIOR_STRICT_MIN_TIRE_LIFE_RATIO = 0.80;
    private static final double PRIOR_STRICT_MIN_TIRE_LIFE_RATIO_LATE = 0.85;
    private static final double PRIOR_STRICT_MIN_TOTAL_SCORE_LATE = 75.0;
    private static final double PRIOR_STRICT_MIN_TIMING_PRESSURE = 9.0;
    private static final int PRIOR_STRICT_WINDOW_OPEN_MIN_PASS = 2;

    private static final int RIVAL_RECENT_PIT_WINDOW_SHORT = 2;
    private static final int RIVAL_RECENT_PIT_WINDOW_LONG = 3;
    private static final double RIVAL_PROMOTION_MAX_GAP_SEC_GREEN = 3.0;
    private static final double RIVAL_PROMOTION_MAX_GAP_SEC_GREEN_RELAXED = 5.0;
    private static final double RIVAL_PROMOTION_MIN_TIMING_PRESSURE = 6.0;
    private static final int C6_DEFAULT_RIVAL_RECENT_MAX_LAPS = 1;
    private static final double C6_DEFAULT_RIVAL_MIN_URGENCY = 10.0;
    private static final double C6_DEFAULT_RIVAL_MIN_TIMING_PRESSURE = 10.0;
    private static final boolean C6_DEFAULT_ULTRA_CLOSE_GAP_GUARD_ENABLED = true;
    private static final double C6_ULTRA_CLOSE_GAP_SEC = 1.5;
    private static final double C6_ULTRA_CLOSE_RATIO_OVERRIDE = 1.20;

    private static final double MAX_TIMING_PRESSURE_SCORE = 10.0;
    private static final double TIMING_PRESSURE_MIN_URGENCY = 8.0;
    private static final double TIMING_PRESSURE_PACE_ACCEL_REFERENCE = 0.003;
    private static final double TIMING_PRESSURE_GAP_EXPANSION_REFERENCE = 0.8;

    // emit-gate: minimum score increase since last emission before re-emitting
    private static final double RE_EMIT_DELTA = 10.0;
    private static final int GOOD_PIT_REEMIT_LAPS = 2;
    private static final int PIT_NOW_REEMIT_LAPS = 2;
    private static final double TIMING_REEMIT_PRESSURE_DELTA = 2.0;
    private static final double TIMING_REEMIT_MIN_URGENCY = 10.0;
    private static final double SLOW_LAP_RATIO_THRESHOLD = 0.005;
    private static final int RECENT_LAP_WINDOW = 3;
    private static final String CURRENT_STATUS_KEY = "current";
    private static final String LATEST_LAP_KEY = "latest";
    private static final double PACE_CURVE_POWER = 1.5;

    // track status score: +60 for sc/vsc (crisp, binary event)
    private static final int TRACK_STATUS_SCORE = 60;

    // pace curve: paceRatio at which score reaches 30 (fully degraded)
    // this calibration follows heilmeier 2020 and carrasco 2023, react at narrower tire fade windows.
    // ex: at 2% degradation vs stint best, pace score reaches the ceiling.
    private static final double PACE_CEILING_RATIO = 0.02;

    // traffic thresholds (seconds)
    private static final double CLEAN_AIR_GAP = 3.0;
    private static final double DRS_THRESHOLD = 1.0;

    // urgency starts later to reduce premature calls, this follows carrasco 2023 window timing logic.
    private static final double URGENCY_ONSET_RATIO = 0.70;

    // tyre life bonus for easy pass of car ahead with old tires
    private static final int EASY_PASS_TYRE_LIFE = 25;
    private static final double EASY_PASS_BONUS = 5.0;

    // minimum tyre age before evaluation is meaningful
    private static final int MIN_TYRE_LIFE = 8;

    // lost chance detection: peak score threshold and drop threshold
    private static final double LOST_CHANCE_PEAK = 70.0;
    private static final double LOST_CHANCE_DROP = 40.0;

    // default max stint estimates per compound when no observation is available yet
    private static final int DEFAULT_SOFT_STINT = 18; //TODO these values may be better as percenteges of race length rather than fixed lap counts
    private static final int DEFAULT_MEDIUM_STINT = 30;
    private static final int DEFAULT_HARD_STINT = 40;
    private static final int DEFAULT_WET_STINT = 25;

    // end-of-race sigmoid: steepness of the logistic curve.
    // k=15 produces a sharp transition centered at the midpoint (0.92),
    // near-zero below 85%, effectively -100 above 98%.
    private static final double EOR_SIGMOID_K = 15.0;

    // end-of-race sigmoid: midpoint of the logistic curve (92% race completion).
    // at this point, penalty = -50. chosen so that the "cliff" where pitting becomes
    // meaningless aligns with ~4 laps remaining in a 50-lap race.
    private static final double EOR_SIGMOID_MIDPOINT = 0.92;

    // per-driver strategy tracking, key = driver abbreviation
    private transient MapState<String, DriverPitState> driverStates;

    // latest event per driver, used to build an always-current full grid snapshot
    private transient MapState<String, LapEvent> latestGridState;

    // global maximum observed stint length per compound across all drivers
    private transient MapState<String, Integer> maxStintByCompound;

    // emit-gate: score at last emission per driver
    private transient MapState<String, Double> lastEmittedScore;

    // emit-gate: stint number at last emission per driver
    private transient MapState<String, Integer> lastEmittedStint;

    // emit-gate: track status at last emission per driver
    private transient MapState<String, String> lastEmittedTrackStatus;

    // emit-gate: lap number at last emission per driver
    private transient MapState<String, Integer> lastEmittedLap;

    // emit-gate: timing pressure at last emission per driver
    private transient MapState<String, Double> lastEmittedTimingPressure;
    private transient MapState<String, String> lastEmittedLabel;

    // per-driver peak score tracking for LOST_CHANCE detection
    private transient MapState<String, Double> peakScores;

    // per-driver flag: whether LOST_CHANCE has been emitted this stint
    private transient MapState<String, Boolean> lostChanceEmitted;

    // cache latest observed race progress for broadcast urgency fast-path
    private transient MapState<String, Integer> lastCompletedLap;

    // episode gating state: one actionable decision episode per driver inside H=2.
    private transient MapState<String, Integer> activeEpisodeStartLap;
    private transient MapState<String, String> episodeCloseReason;
    private transient MapState<String, Integer> lastObservedPitLap;

    // max observed lap across all events, used as a stall-safe progress trigger
    private transient ValueState<Integer> maxLapState;
    private transient Map<String, PriorStats> pitWindowPriors;
    private transient boolean pitWindowPriorsAvailable;
    private transient String pitWindowPriorsStatus;
    private transient String pitWindowPriorsPath;
    private transient String pitWindowPriorsLoadedAt;
    private transient boolean priorPromotionEnabled;
    private transient boolean priorPromotionStrictMode;
    private transient boolean rivalPressurePromotionEnabled;
    private transient boolean rivalPressureCautionEnabled;
    private transient boolean priorSuppressionEnabled;
    private transient boolean c6TunedRivalProfileEnabled;
    private transient int rivalRecentMaxLaps;
    private transient double rivalMinUrgency;
    private transient double rivalMinTimingPressure;
    private transient boolean rivalUltraCloseGapGuardEnabled;

    private enum SemanticLabel {
        MONITOR,
        OPPORTUNITY,
        PIT_NOW,
        LOST_CHANCE
    }

    private enum PitWindowPhase {
        TOO_EARLY,
        WINDOW_OPEN,
        LATE,
        OVERDUE,
        UNKNOWN
    }

    private enum PriorWindowPhase {
        TOO_EARLY,
        WINDOW_OPEN,
        LATE_WINDOW,
        OVERDUE,
        UNKNOWN
    }

    private static final class PriorStats {
        private final String keyType;
        private final String key;
        private final int sampleCount;
        private final String priorConfidence;
        private final Double progressQ25;
        private final Double progressQ50;
        private final Double progressQ75;
        private final Double progressQ90;
        private final Double tyreQ25;
        private final Double tyreQ50;
        private final Double tyreQ75;
        private final Double tyreQ90;

        private PriorStats(
                String keyType,
                String key,
                int sampleCount,
                String priorConfidence,
                Double progressQ25,
                Double progressQ50,
                Double progressQ75,
                Double progressQ90,
                Double tyreQ25,
                Double tyreQ50,
                Double tyreQ75,
                Double tyreQ90) {
            this.keyType = keyType;
            this.key = key;
            this.sampleCount = sampleCount;
            this.priorConfidence = priorConfidence;
            this.progressQ25 = progressQ25;
            this.progressQ50 = progressQ50;
            this.progressQ75 = progressQ75;
            this.progressQ90 = progressQ90;
            this.tyreQ25 = tyreQ25;
            this.tyreQ50 = tyreQ50;
            this.tyreQ75 = tyreQ75;
            this.tyreQ90 = tyreQ90;
        }
    }

    private static final class PriorMatch {
        private final PriorStats prior;
        private final String priorKeyUsed;
        private final String fallbackLevel;

        private PriorMatch(PriorStats prior, String priorKeyUsed, String fallbackLevel) {
            this.prior = prior;
            this.priorKeyUsed = priorKeyUsed;
            this.fallbackLevel = fallbackLevel;
        }
    }

    private static final class PriorPromotionDecision {
        private final boolean priorPromotionApplied;
        private final String priorPromotionReason;
        private final String priorPromotionSkippedReason;
        private final String priorKeyUsed;
        private final String fallbackLevel;
        private final String priorConfidence;
        private final int priorSampleCount;
        private final PriorWindowPhase priorWindowPhase;
        private final Double priorProgressQ25;
        private final Double priorProgressQ50;
        private final Double priorProgressQ75;
        private final Double priorProgressQ90;
        private final Double priorTyreQ25;
        private final Double priorTyreQ50;
        private final Double priorTyreQ75;
        private final Double priorTyreQ90;

        private PriorPromotionDecision(
                boolean priorPromotionApplied,
                String priorPromotionReason,
                String priorPromotionSkippedReason,
                String priorKeyUsed,
                String fallbackLevel,
                String priorConfidence,
                int priorSampleCount,
                PriorWindowPhase priorWindowPhase,
                Double priorProgressQ25,
                Double priorProgressQ50,
                Double priorProgressQ75,
                Double priorProgressQ90,
                Double priorTyreQ25,
                Double priorTyreQ50,
                Double priorTyreQ75,
                Double priorTyreQ90) {
            this.priorPromotionApplied = priorPromotionApplied;
            this.priorPromotionReason = priorPromotionReason;
            this.priorPromotionSkippedReason = priorPromotionSkippedReason;
            this.priorKeyUsed = priorKeyUsed;
            this.fallbackLevel = fallbackLevel;
            this.priorConfidence = priorConfidence;
            this.priorSampleCount = priorSampleCount;
            this.priorWindowPhase = priorWindowPhase;
            this.priorProgressQ25 = priorProgressQ25;
            this.priorProgressQ50 = priorProgressQ50;
            this.priorProgressQ75 = priorProgressQ75;
            this.priorProgressQ90 = priorProgressQ90;
            this.priorTyreQ25 = priorTyreQ25;
            this.priorTyreQ50 = priorTyreQ50;
            this.priorTyreQ75 = priorTyreQ75;
            this.priorTyreQ90 = priorTyreQ90;
        }

        private static PriorPromotionDecision unavailable(String reason) {
            return new PriorPromotionDecision(
                    false,
                    reason,
                    "",
                    "",
                    "",
                    "UNKNOWN",
                    0,
                    PriorWindowPhase.UNKNOWN,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
        }
    }

    private static final class RivalPromotionDecision {
        private final boolean rivalPressureApplied;
        private final String rivalPressureReason;
        private final String rivalPressureSource;
        private final String classificationAheadDriver;
        private final String classificationBehindDriver;
        private final Double classificationGapAheadSec;
        private final Double classificationGapBehindSec;
        private final int aheadPittedLastNLaps;
        private final int behindPittedLastNLaps;
        private final int teammatePittedLastNLaps;
        private final boolean ultraCloseGuardApplied;

        private RivalPromotionDecision(
                boolean rivalPressureApplied,
                String rivalPressureReason,
                String rivalPressureSource,
                String classificationAheadDriver,
                String classificationBehindDriver,
                Double classificationGapAheadSec,
                Double classificationGapBehindSec,
                int aheadPittedLastNLaps,
                int behindPittedLastNLaps,
                int teammatePittedLastNLaps) {
            this(
                    rivalPressureApplied,
                    rivalPressureReason,
                    rivalPressureSource,
                    classificationAheadDriver,
                    classificationBehindDriver,
                    classificationGapAheadSec,
                    classificationGapBehindSec,
                    aheadPittedLastNLaps,
                    behindPittedLastNLaps,
                    teammatePittedLastNLaps,
                    false);
        }

        private RivalPromotionDecision(
                boolean rivalPressureApplied,
                String rivalPressureReason,
                String rivalPressureSource,
                String classificationAheadDriver,
                String classificationBehindDriver,
                Double classificationGapAheadSec,
                Double classificationGapBehindSec,
                int aheadPittedLastNLaps,
                int behindPittedLastNLaps,
                int teammatePittedLastNLaps,
                boolean ultraCloseGuardApplied) {
            this.rivalPressureApplied = rivalPressureApplied;
            this.rivalPressureReason = rivalPressureReason;
            this.rivalPressureSource = rivalPressureSource;
            this.classificationAheadDriver = classificationAheadDriver;
            this.classificationBehindDriver = classificationBehindDriver;
            this.classificationGapAheadSec = classificationGapAheadSec;
            this.classificationGapBehindSec = classificationGapBehindSec;
            this.aheadPittedLastNLaps = aheadPittedLastNLaps;
            this.behindPittedLastNLaps = behindPittedLastNLaps;
            this.teammatePittedLastNLaps = teammatePittedLastNLaps;
            this.ultraCloseGuardApplied = ultraCloseGuardApplied;
        }

        private static RivalPromotionDecision unavailable(String reason) {
            return new RivalPromotionDecision(
                    false,
                    reason,
                    "",
                    "",
                    "",
                    null,
                    null,
                    -1,
                    -1,
                    -1);
        }
    }

    private static final class TimingPressureInfo {
        private final boolean available;
        private final double score;

        private TimingPressureInfo(boolean available, double score) {
            this.available = available;
            this.score = score;
        }
    }

    private static final class C6RivalFilterDecision {
        private final boolean pass;
        private final String reason;
        private final boolean ultraCloseGuardApplied;

        private C6RivalFilterDecision(boolean pass, String reason, boolean ultraCloseGuardApplied) {
            this.pass = pass;
            this.reason = reason;
            this.ultraCloseGuardApplied = ultraCloseGuardApplied;
        }
    }

    private static final class TimingGateDecision {
        private final SuggestionLabel originalLabel;
        private final SuggestionLabel legacyLabel;
        private final SemanticLabel semanticLabel;
        private final boolean timingGatePassed;
        private final String timingGateReason;
        private final String finalDecisionReason;
        private final PitWindowPhase pitWindowPhase;
        private final boolean weakTimingCombo;
        private final boolean earlyWindow;

        private TimingGateDecision(
                SuggestionLabel originalLabel,
                SuggestionLabel legacyLabel,
                SemanticLabel semanticLabel,
                boolean timingGatePassed,
                String timingGateReason,
                String finalDecisionReason,
                PitWindowPhase pitWindowPhase,
                boolean weakTimingCombo,
                boolean earlyWindow) {
            this.originalLabel = originalLabel;
            this.legacyLabel = legacyLabel;
            this.semanticLabel = semanticLabel;
            this.timingGatePassed = timingGatePassed;
            this.timingGateReason = timingGateReason;
            this.finalDecisionReason = finalDecisionReason;
            this.pitWindowPhase = pitWindowPhase;
            this.weakTimingCombo = weakTimingCombo;
            this.earlyWindow = earlyWindow;
        }
    }

    @Override
    public void open(OpenContext openContext) {
        // 2h ttl: prevents unbounded state growth over continuous streaming
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, DriverPitState> driverDesc
                = new MapStateDescriptor<>("strategy-driver-states", String.class, DriverPitState.class);
        driverDesc.enableTimeToLive(ttlConfig);
        driverStates = getRuntimeContext().getMapState(driverDesc);

        MapStateDescriptor<String, LapEvent> latestGridDesc
                = new MapStateDescriptor<>("strategy-latest-grid", Types.STRING, Types.POJO(LapEvent.class));
        latestGridDesc.enableTimeToLive(ttlConfig);
        latestGridState = getRuntimeContext().getMapState(latestGridDesc);

        MapStateDescriptor<String, Integer> maxStintDesc
                = new MapStateDescriptor<>("strategy-max-stint", Types.STRING, Types.INT);
        maxStintDesc.enableTimeToLive(ttlConfig);
        maxStintByCompound = getRuntimeContext().getMapState(maxStintDesc);

        MapStateDescriptor<String, Double> emitScoreDesc
                = new MapStateDescriptor<>("strategy-emit-score", String.class, Double.class);
        emitScoreDesc.enableTimeToLive(ttlConfig);
        lastEmittedScore = getRuntimeContext().getMapState(emitScoreDesc);

        MapStateDescriptor<String, Integer> emitStintDesc
                = new MapStateDescriptor<>("strategy-emit-stint", Types.STRING, Types.INT);
        emitStintDesc.enableTimeToLive(ttlConfig);
        lastEmittedStint = getRuntimeContext().getMapState(emitStintDesc);

        MapStateDescriptor<String, String> emitTsDesc
                = new MapStateDescriptor<>("strategy-emit-track-status", Types.STRING, Types.STRING);
        emitTsDesc.enableTimeToLive(ttlConfig);
        lastEmittedTrackStatus = getRuntimeContext().getMapState(emitTsDesc);

        MapStateDescriptor<String, Integer> emitLapDesc
            = new MapStateDescriptor<>("strategy-emit-lap", Types.STRING, Types.INT);
        emitLapDesc.enableTimeToLive(ttlConfig);
        lastEmittedLap = getRuntimeContext().getMapState(emitLapDesc);

        MapStateDescriptor<String, Double> emitTimingPressureDesc
            = new MapStateDescriptor<>("strategy-emit-timing-pressure", Types.STRING, Types.DOUBLE);
        emitTimingPressureDesc.enableTimeToLive(ttlConfig);
        lastEmittedTimingPressure = getRuntimeContext().getMapState(emitTimingPressureDesc);

        MapStateDescriptor<String, String> emitLabelDesc
            = new MapStateDescriptor<>("strategy-emit-label", Types.STRING, Types.STRING);
        emitLabelDesc.enableTimeToLive(ttlConfig);
        lastEmittedLabel = getRuntimeContext().getMapState(emitLabelDesc);

        MapStateDescriptor<String, Double> peakDesc
                = new MapStateDescriptor<>("strategy-peak-scores", String.class, Double.class);
        peakDesc.enableTimeToLive(ttlConfig);
        peakScores = getRuntimeContext().getMapState(peakDesc);

        MapStateDescriptor<String, Boolean> lostDesc
                = new MapStateDescriptor<>("strategy-lost-chance", String.class, Boolean.class);
        lostDesc.enableTimeToLive(ttlConfig);
        lostChanceEmitted = getRuntimeContext().getMapState(lostDesc);

        MapStateDescriptor<String, Integer> lastLapDesc
                = new MapStateDescriptor<>("strategy-last-completed-lap", Types.STRING, Types.INT);
        lastLapDesc.enableTimeToLive(ttlConfig);
        lastCompletedLap = getRuntimeContext().getMapState(lastLapDesc);

        MapStateDescriptor<String, Integer> episodeStartDesc
                = new MapStateDescriptor<>("strategy-active-episode-start-lap", Types.STRING, Types.INT);
        episodeStartDesc.enableTimeToLive(ttlConfig);
        activeEpisodeStartLap = getRuntimeContext().getMapState(episodeStartDesc);

        MapStateDescriptor<String, String> episodeReasonDesc
                = new MapStateDescriptor<>("strategy-episode-close-reason", Types.STRING, Types.STRING);
        episodeReasonDesc.enableTimeToLive(ttlConfig);
        episodeCloseReason = getRuntimeContext().getMapState(episodeReasonDesc);

        MapStateDescriptor<String, Integer> lastPitLapDesc
                = new MapStateDescriptor<>("strategy-last-observed-pit-lap", Types.STRING, Types.INT);
        lastPitLapDesc.enableTimeToLive(ttlConfig);
        lastObservedPitLap = getRuntimeContext().getMapState(lastPitLapDesc);

        ValueStateDescriptor<Integer> maxLapDesc
                = new ValueStateDescriptor<>("strategy-max-lap", Types.INT);
        maxLapDesc.enableTimeToLive(ttlConfig);
        maxLapState = getRuntimeContext().getState(maxLapDesc);

        priorPromotionEnabled = readBooleanSetting(FLAG_PRIOR_PROMOTION_ENABLED, false);
        priorPromotionStrictMode = readBooleanSetting(FLAG_PRIOR_PROMOTION_STRICT_MODE, true);
        rivalPressurePromotionEnabled = readBooleanSetting(FLAG_RIVAL_PRESSURE_ENABLED, false);
        rivalPressureCautionEnabled = readBooleanSetting(FLAG_RIVAL_PRESSURE_CAUTION_ENABLED, false);
        priorSuppressionEnabled = readBooleanSetting(FLAG_PRIOR_SUPPRESSION_ENABLED, false);
        c6TunedRivalProfileEnabled = readBooleanSetting(FLAG_C6_TUNED_RIVAL_PROFILE_ENABLED, false);
        rivalRecentMaxLaps = Math.max(0, readIntSetting(SETTING_RIVAL_RECENT_MAX_LAPS, C6_DEFAULT_RIVAL_RECENT_MAX_LAPS));
        rivalMinUrgency = readDoubleSetting(SETTING_RIVAL_MIN_URGENCY, C6_DEFAULT_RIVAL_MIN_URGENCY);
        rivalMinTimingPressure = readDoubleSetting(SETTING_RIVAL_MIN_TIMING_PRESSURE, C6_DEFAULT_RIVAL_MIN_TIMING_PRESSURE);
        rivalUltraCloseGapGuardEnabled = readBooleanSetting(
                SETTING_RIVAL_ULTRA_CLOSE_GAP_GUARD_ENABLED,
                C6_DEFAULT_ULTRA_CLOSE_GAP_GUARD_ENABLED);

        LOG.info(
                "SDE config loaded: priorPromotionEnabled={}, priorPromotionStrictMode={}, rivalPressurePromotionEnabled={}, "
                        + "rivalPressureCautionEnabled={}, priorSuppressionEnabled={}, c6TunedProfileEnabled={}, "
                        + "rivalRecentMaxLaps={}, rivalMinUrgency={}, rivalMinTimingPressure={}, "
                        + "rivalUltraCloseGapGuardEnabled={}",
                priorPromotionEnabled,
                priorPromotionStrictMode,
                rivalPressurePromotionEnabled,
                rivalPressureCautionEnabled,
                priorSuppressionEnabled,
                c6TunedRivalProfileEnabled,
                rivalRecentMaxLaps,
                rivalMinUrgency,
                rivalMinTimingPressure,
                rivalUltraCloseGapGuardEnabled);

        loadPitWindowPriors();
    }

    // data-driven evaluation path, updates snapshot and triggers scoring on race progress
    @Override
    public void processElement(LapEvent event,
            KeyedBroadcastProcessFunction<String, LapEvent, TrackStatusEvent, PitSuggestionAlert>.ReadOnlyContext ctx,
            Collector<PitSuggestionAlert> out) throws Exception {
        if (event == null || event.getDriver() == null || event.getLapNumber() <= 0) {
            return;
        }

        int lap = event.getLapNumber();
        String driver = event.getDriver();

        latestGridState.put(driver, event);
        updateMaxStint(event);
        updateDriverState(event);
        closeDecisionEpisodeOnPit(event);
        closeExpiredDecisionEpisodes(event.getLapNumber());

        Integer maxLap = maxLapState.value();
        if (maxLap == null) {
            maxLap = 0;
        }

        if (lap > maxLap) {
            maxLapState.update(lap);
            if (lap > 1) {
                int previousLap = lap - 1;
                lastCompletedLap.put(LATEST_LAP_KEY, previousLap);
                List<LapEvent> currentGrid = collectFreshGrid(lap);
                if (!currentGrid.isEmpty()) {
                    String trackStatus = readTrackStatus(ctx);
                    evaluateAll(currentGrid, trackStatus, out);
                }
            }
        }
    }

    // broadcast-driven urgency path, fires immediately when SC/VSC deploys
    // re-evaluates the filtered physical-grid snapshot with current track status
    @Override
    public void processBroadcastElement(TrackStatusEvent statusEvent,
            KeyedBroadcastProcessFunction<String, LapEvent, TrackStatusEvent, PitSuggestionAlert>.Context ctx,
            Collector<PitSuggestionAlert> out) throws Exception {
        if (statusEvent == null || statusEvent.getStatus() == null) {
            return;
        }

        ctx.getBroadcastState(TRACK_STATUS_STATE).put(CURRENT_STATUS_KEY, statusEvent.getStatus());

        String status = statusEvent.getStatus();
        if (!TrackStatusCodes.isCaution(status)) {
            return; // only trigger urgency on SC/VSC/VSCEnding
        }

        LOG.info("sc/vsc urgency trigger: status={}", status);

        // re-evaluate all drivers using latest observed race progress
        Integer latestLap = lastCompletedLap.get(LATEST_LAP_KEY);
        if (latestLap == null) {
            return;
        }

        List<LapEvent> currentGrid = collectFreshGrid(latestLap);
        if (!currentGrid.isEmpty()) {
            evaluateAll(currentGrid, status, out);
        }
    }

    // reads current track status from broadcast state, defaulting to green
    private String readTrackStatus(
            KeyedBroadcastProcessFunction<String, LapEvent, TrackStatusEvent, PitSuggestionAlert>.ReadOnlyContext ctx)
            throws Exception {
        String status = ctx.getBroadcastState(TRACK_STATUS_STATE).get(CURRENT_STATUS_KEY);
        return TrackStatusCodes.normalizeOrGreen(status);
    }

    // updates max observed tyre life per compound in real time, each lap
    private void updateMaxStint(LapEvent event) throws Exception {
        String compound = event.getCompound();
        if (compound == null) {
            return;
        }

        Integer currentMax = maxStintByCompound.get(compound);
        int tyreLife = event.getTyreLife();
        if (currentMax == null || tyreLife > currentMax) {
            maxStintByCompound.put(compound, tyreLife);
        }
    }

    // updates per-driver state: stint transitions, stint best lap, pace tracking
    private void updateDriverState(LapEvent event) throws Exception {
        String driver = event.getDriver();
        DriverPitState state = driverStates.get(driver);
        if (state == null) {
            state = new DriverPitState();
        }

        // stint change: reset pace tracking, peak score, lost chance flag
        if (state.getCurrentStint() != event.getStint()) {
            state.setCurrentStint(event.getStint());
            state.setStintBestLap(Double.MAX_VALUE);
            state.setConsecutiveSlowLaps(0);
            state.setLastPaceRatio(0.0);
            state.setPaceRatioDelta(0.0);
            state.setLastGapToCarAhead(null);
            state.setGapToCarAheadDelta(0.0);
            peakScores.put(driver, 0.0);
            lostChanceEmitted.put(driver, false);
            activeEpisodeStartLap.remove(driver);
            episodeCloseReason.put(driver, "stint_change");
        }

        state.setLastCompound(event.getCompound());
        state.setLastTyreLife(event.getTyreLife());
        state.setPaceRatioDelta(0.0);
        state.setGapToCarAheadDelta(0.0);

        Double lapTime = event.getLapTime();
        if (lapTime != null && lapTime > 0
                && event.getPitInTime() == null && event.getPitOutTime() == null
                && TrackStatusCodes.isGreenOrUnknown(event.getTrackStatus())) {
            double previousPaceRatio = state.getLastPaceRatio();
            if (lapTime < state.getStintBestLap()) {
                state.setStintBestLap(lapTime);
            }

            // pace ratio for continuous scoring
            if (state.getStintBestLap() > 0 && state.getStintBestLap() < Double.MAX_VALUE) {
                double currentPaceRatio = (lapTime - state.getStintBestLap()) / state.getStintBestLap();
                state.setLastPaceRatio(currentPaceRatio);
                state.setPaceRatioDelta(currentPaceRatio - previousPaceRatio);
            }

            // track consecutive slow laps (still needed for filtering one-off blips)
            if (state.getLastPaceRatio() > SLOW_LAP_RATIO_THRESHOLD) {
                state.setConsecutiveSlowLaps(state.getConsecutiveSlowLaps() + 1);
            } else {
                state.setConsecutiveSlowLaps(0);
            }
        }

        if (event.getPitInTime() == null && event.getPitOutTime() == null
                && TrackStatusCodes.isGreenOrUnknown(event.getTrackStatus())
                && event.getGapToCarAhead() != null
                && event.getGapToCarAhead() >= 0.0) {
            Double previousGap = state.getLastGapToCarAhead();
            double currentGap = event.getGapToCarAhead();
            state.setLastGapToCarAhead(currentGap);
            if (previousGap != null) {
                state.setGapToCarAheadDelta(currentGap - previousGap);
            }
        }

        driverStates.put(driver, state);
    }

    private List<LapEvent> collectFreshGrid(int leaderLap) throws Exception {
        List<LapEvent> currentGrid = new ArrayList<>();
        List<String> staleDrivers = new ArrayList<>();

        for (LapEvent e : latestGridState.values()) {
            if (e.getLapNumber() >= leaderLap - RECENT_LAP_WINDOW) {
                currentGrid.add(e);
            }
            if (e.getLapNumber() < leaderLap - RECENT_LAP_WINDOW) {
                staleDrivers.add(e.getDriver());
            }
        }

        for (String staleDriver : staleDrivers) {
            latestGridState.remove(staleDriver);
        }

        return currentGrid;
    }

    // evaluates the pit desirability score for each eligible driver
    private void evaluateAll(List<LapEvent> laps, String currentTrackStatus,
            Collector<PitSuggestionAlert> out) throws Exception {
        laps.sort(Comparator.comparingInt(LapEvent::getPosition));

        for (int i = 0; i < laps.size(); i++) {
            LapEvent current = laps.get(i);
            String driver = current.getDriver();
            DriverPitState driverState = driverStates.get(driver);

            // skip fresh tires, pit laps
            if (current.getTyreLife() < MIN_TYRE_LIFE) {
                continue;
            }
            if (current.getPitInTime() != null || current.getPitOutTime() != null) {
                continue;
            }

            double paceScore = computePaceScore(current);
            int trackStatusScore = computeTrackStatusScore(currentTrackStatus);
            TrafficResult traffic = computeTrafficResult(current, laps, i, currentTrackStatus);
            double strategyPenalty = computeStrategyPenalty(current);
            double urgencyScore = computeUrgencyScore(current);
            double rivalPitReactionBoost = computeRivalPitReactionBoost(current, laps, i);
            double actionUrgencyScore = Math.min(30.0, urgencyScore + rivalPitReactionBoost);
            double competitivePressure = computeCompetitivePressure(current, laps, i);
            double signalConfidence = computeSignalConfidence(current, laps, i, driverState);
            double uncertaintyPenalty = computeUncertaintyPenalty(signalConfidence, currentTrackStatus);
            TimingPressureInfo timingPressureInfo = resolveTimingPressureInfo(
                    driverState,
                    urgencyScore,
                    strategyPenalty,
                    traffic.score,
                    trackStatusScore,
                    rivalPitReactionBoost);
            double timingPressureScore = timingPressureInfo.score;
            double endOfRacePenalty = computeEndOfRacePenalty(current);

            double totalScore = paceScore + trackStatusScore + traffic.score
                    + strategyPenalty + actionUrgencyScore + timingPressureScore + endOfRacePenalty
                    + competitivePressure - uncertaintyPenalty;
            totalScore = Math.max(0.0, Math.min(100.0, totalScore));

            // update peak score for lost_chance detection
            Double peak = peakScores.get(driver);
            if (peak == null) {
                peak = 0.0;
            }
            if (totalScore > peak) {
                peakScores.put(driver, totalScore);
                peak = totalScore;
            }

            // lost_chance detection: peak was >= 70 but score dropped below 40
            Boolean lostEmitted = lostChanceEmitted.get(driver);
            if (lostEmitted == null) {
                lostEmitted = false;
            }

            if (!lostEmitted && peak >= LOST_CHANCE_PEAK && totalScore < LOST_CHANCE_DROP) {
                DriverPitState ds = driverStates.get(driver);
                // only emit if degradation is still worsening (not improvement from new tires)
                if (ds != null && ds.getLastPaceRatio() > SLOW_LAP_RATIO_THRESHOLD) {
                    lostChanceEmitted.put(driver, true);
                    TimingGateDecision lostChanceDecision = passthroughTimingDecision(
                            SuggestionLabel.LOST_CHANCE,
                            "LOST_CHANCE_EMISSION");
                    emitAlert(current, totalScore, paceScore, trackStatusScore,
                            traffic, strategyPenalty, urgencyScore, endOfRacePenalty,
                            currentTrackStatus, 0.0, 0.0, SuggestionLabel.LOST_CHANCE, lostChanceDecision,
                            PriorPromotionDecision.unavailable("NOT_APPLICABLE"),
                            RivalPromotionDecision.unavailable("NOT_APPLICABLE"), out);
                    continue;
                }
            }

            if (totalScore < EMIT_THRESHOLD) {
                continue;
            }

            SuggestionLabel label = classifyScore(totalScore);
            if (shouldPromoteMonitorToGoodPit(
                    label, totalScore, paceScore, actionUrgencyScore, trackStatusScore)) {
                label = SuggestionLabel.GOOD_PIT;
            }
            label = applyTrafficAwareActionabilityGate(label, traffic.score, urgencyScore);
            double paceRatioDelta = driverState != null ? driverState.getPaceRatioDelta() : 0.0;
            label = applyEarlyActionabilityGate(
                    label,
                    trackStatusScore,
                    actionUrgencyScore,
                    strategyPenalty,
                    paceScore,
                    rivalPitReactionBoost,
                    timingPressureScore,
                    paceRatioDelta);
            label = applyAdaptiveRegimeActionabilityGate(
                    label,
                    totalScore,
                    current,
                    currentTrackStatus,
                    signalConfidence);

            TimingGateDecision timingDecision = applyDeterministicTimingGate(
                    label,
                    totalScore,
                    actionUrgencyScore,
                    current.getTyreLife(),
                    current.getCompound(),
                    current.getLapNumber(),
                    current.getTotalLaps(),
                    currentTrackStatus,
                    timingPressureInfo.available,
                    timingPressureScore);
            label = timingDecision.legacyLabel;
            PriorPromotionDecision priorPromotionDecision = evaluatePriorPromotionDecision(
                    priorPromotionEnabled,
                    priorPromotionStrictMode,
                    pitWindowPriorsAvailable,
                    pitWindowPriors,
                    timingDecision,
                    current,
                    currentTrackStatus,
                    actionUrgencyScore,
                    totalScore,
                    timingPressureInfo.available,
                    timingPressureScore);
            if (priorPromotionDecision.priorPromotionApplied) {
                timingDecision = toPriorPromotedDecision(timingDecision);
                label = timingDecision.legacyLabel;
            }
            RivalPromotionDecision rivalPromotionDecision = evaluateRivalPromotionDecision(
                    rivalPressurePromotionEnabled,
                    rivalPressureCautionEnabled,
                    timingDecision,
                    current,
                    laps,
                    i,
                    currentTrackStatus,
                    actionUrgencyScore,
                    timingPressureInfo.available,
                    timingPressureScore);
            if (rivalPromotionDecision.rivalPressureApplied) {
                timingDecision = toRivalPromotedDecision(timingDecision);
                label = timingDecision.legacyLabel;
            }

            // emit-gate: check if we should suppress this alert
            if (!shouldEmit(driver, current.getStint(), current.getLapNumber(),
                    totalScore, currentTrackStatus, label, timingPressureScore, actionUrgencyScore)) {
                continue;
            }

            if (isPitNowLabel(label) && isDecisionEpisodeActive(driver, current.getLapNumber())) {
                continue;
            }

            // update emit-gate tracking
            lastEmittedScore.put(driver, totalScore);
            lastEmittedStint.put(driver, current.getStint());
            lastEmittedTrackStatus.put(driver, currentTrackStatus);
            lastEmittedLap.put(driver, current.getLapNumber());
            lastEmittedTimingPressure.put(driver, timingPressureScore);
            lastEmittedLabel.put(driver, label.name());
            if (isPitNowLabel(label)) {
                openDecisionEpisode(driver, current.getLapNumber());
            }

            emitAlert(current, totalScore, paceScore, trackStatusScore,
                    traffic, strategyPenalty, actionUrgencyScore, endOfRacePenalty,
                    currentTrackStatus, rivalPitReactionBoost, timingPressureScore, label, timingDecision,
                    priorPromotionDecision, rivalPromotionDecision, out);
        }
    }

    private void emitAlert(LapEvent current, double totalScore, double paceScore,
            int trackStatusScore, TrafficResult traffic, double strategyPenalty,
            double urgencyScore, double endOfRacePenalty, String trackStatus,
            double rivalPitReactionBoost,
            double timingPressureScore,
            SuggestionLabel label,
            TimingGateDecision timingDecision,
            PriorPromotionDecision priorPromotionDecision,
            RivalPromotionDecision rivalPromotionDecision,
            Collector<PitSuggestionAlert> out) {

        String suggestion = buildSuggestion(paceScore, trackStatusScore,
                traffic.score, strategyPenalty, urgencyScore, endOfRacePenalty,
                rivalPitReactionBoost, timingPressureScore);

        TimingGateDecision safeDecision = timingDecision != null
                ? timingDecision
                : passthroughTimingDecision(label, "NO_TIMING_GATE");
        PriorPromotionDecision safePrior = priorPromotionDecision != null
                ? priorPromotionDecision
                : PriorPromotionDecision.unavailable("PRIORS_UNAVAILABLE");
        RivalPromotionDecision safeRival = rivalPromotionDecision != null
                ? rivalPromotionDecision
                : RivalPromotionDecision.unavailable("RIVAL_PROMOTION_DISABLED");
        String decisionMetadataJson = buildDecisionMetadataJson(
                current,
                totalScore,
                urgencyScore,
                timingPressureScore,
                safeDecision,
                safePrior,
                safeRival);

        out.collect(new PitSuggestionAlert(
                current.getRace(),
                current.getDriver(),
                current.getLapNumber(),
                current.getDate(),
                current.getPosition(),
                current.getCompound(),
                current.getTyreLife(),
                totalScore,
                paceScore,
                trackStatusScore,
                traffic.score,
                strategyPenalty,
                urgencyScore,
                endOfRacePenalty,
                trackStatus,
                traffic.emergencePosition,
                traffic.gapToPhysicalCar,
                label.name(),
                suggestion,
                safeDecision.semanticLabel.name(),
                safeDecision.originalLabel.name(),
                safeDecision.finalDecisionReason,
                safeDecision.timingGatePassed,
                safeDecision.timingGateReason,
                safeDecision.pitWindowPhase.name(),
                decisionMetadataJson
        ));

        LOG.info("pit strategy: {} lap {} -> {} (score={})",
                current.getDriver(), current.getLapNumber(), label,
                String.format("%.1f", totalScore));
    }

    // emit-gate: suppresses re-emission unless score escalated or track status changed
    private boolean shouldEmit(String driver, int currentStint, int currentLap, double currentScore,
            String currentTrackStatus, SuggestionLabel currentLabel,
            double currentTimingPressure, double urgencyScore) throws Exception {
        Integer prevStint = lastEmittedStint.get(driver);
        Double prevScore = lastEmittedScore.get(driver);
        Integer prevLap = lastEmittedLap.get(driver);
        Double prevTimingPressure = lastEmittedTimingPressure.get(driver);

        // first emission ever, or new stint -> always emit
        if (prevStint == null || prevStint != currentStint) {
            return true;
        }
        if (prevScore == null) {
            return true;
        }

        SuggestionLabel prevLabel = null;
        String prevLabelText = lastEmittedLabel.get(driver);
        if (prevLabelText != null && !prevLabelText.isBlank()) {
            try {
                prevLabel = SuggestionLabel.valueOf(prevLabelText);
            } catch (IllegalArgumentException ignored) {
                prevLabel = null;
            }
        }
        if (prevLabel == null) {
            prevLabel = classifyScore(prevScore);
        }

        // class changed, always emit to keep dashboard and ml timeline aligned
        if (currentLabel != prevLabel) {
            return true;
        }

        // track status changed -> re-emit (new opportunity)
        String prevTrackStatus = lastEmittedTrackStatus.get(driver);
        if (prevTrackStatus != null && !prevTrackStatus.equals(currentTrackStatus)) {
            return true;
        }

        // score moved enough in either direction, emit escalation or downgrade
        if (Math.abs(currentScore - prevScore) >= RE_EMIT_DELTA) {
            return true;
        }

        if ((currentLabel == SuggestionLabel.PIT_NOW || currentLabel == SuggestionLabel.GOOD_PIT)
                && prevTimingPressure != null
                && urgencyScore >= TIMING_REEMIT_MIN_URGENCY
                && (currentTimingPressure - prevTimingPressure) >= TIMING_REEMIT_PRESSURE_DELTA) {
            return true;
        }

        if (currentLabel == SuggestionLabel.PIT_NOW
                && prevLabel == SuggestionLabel.PIT_NOW
                && prevLap != null) {
            return (currentLap - prevLap) >= PIT_NOW_REEMIT_LAPS;
        }

        // this follows event-time window persistence, keep GOOD_PIT visible every few laps.
        return currentLabel == SuggestionLabel.GOOD_PIT
                && prevLabel == SuggestionLabel.GOOD_PIT
                && prevLap != null
                && (currentLap - prevLap) >= GOOD_PIT_REEMIT_LAPS;
    }

    // classifies continuous score into discrete label for pit wall decision
    private static SuggestionLabel classifyScore(double score) {
        if (score >= PIT_NOW_THRESHOLD) {
            return SuggestionLabel.PIT_NOW;
        }
        if (score >= GOOD_PIT_THRESHOLD) {
            return SuggestionLabel.GOOD_PIT;
        }
        return SuggestionLabel.MONITOR;
    }

    // this reasoning is from heilmeier 2020 and carrasco 2023,
    // promote near-threshold monitor to good_pit only when tire and pace signals agree.
    private static boolean shouldPromoteMonitorToGoodPit(
            SuggestionLabel label,
            double totalScore,
            double paceScore,
            double urgencyScore,
            int trackStatusScore) {
        if (label != SuggestionLabel.MONITOR) {
            return false;
        }
        if (trackStatusScore > 0) {
            return false;
        }
        return totalScore >= PROMOTED_GOOD_PIT_MIN_SCORE
                && paceScore >= PROMOTED_GOOD_PIT_MIN_PACE
                && urgencyScore >= PROMOTED_GOOD_PIT_MIN_URGENCY;
    }

    // pass c gate, avoid actionable calls when post pit traffic is bad,
    // unless urgency is critical and waiting is no longer realistic.
    private static SuggestionLabel applyTrafficAwareActionabilityGate(
            SuggestionLabel label,
            double trafficScore,
            double urgencyScore) {
        if (label != SuggestionLabel.PIT_NOW && label != SuggestionLabel.GOOD_PIT) {
            return label;
        }
        if (trafficScore >= TRAFFIC_BLOCK_THRESHOLD) {
            return label;
        }
        if (urgencyScore >= CRITICAL_URGENCY_SCORE) {
            return label;
        }
        return SuggestionLabel.MONITOR;
    }

    // if actionable intent appears too early with weak tire and strategy pressure,
    // keep the alert as monitor until the window becomes decision-relevant.
    private static SuggestionLabel applyEarlyActionabilityGate(
            SuggestionLabel label,
            int trackStatusScore,
            double urgencyScore,
            double strategyPenalty,
            double paceScore,
            double rivalPitReactionBoost,
            double timingPressureScore,
            double paceRatioDelta) {
        if (label != SuggestionLabel.PIT_NOW && label != SuggestionLabel.GOOD_PIT) {
            return label;
        }
        if (trackStatusScore > 0 || rivalPitReactionBoost > 0.0) {
            return label;
        }
        boolean urgencyReady = urgencyScore >= EARLY_ACTION_MIN_URGENCY;
        boolean strategyReady = strategyPenalty <= EARLY_ACTION_MAX_STRATEGY_PENALTY;
        boolean paceEscalation = paceScore >= EARLY_ACTION_MIN_PACE_ESCALATION;
        boolean timingReady = timingPressureScore >= EARLY_ACTION_MIN_TIMING_PRESSURE;
        boolean paceAccelerating = paceRatioDelta >= EARLY_ACTION_MIN_PACE_ACCELERATION;

        if (urgencyReady && (strategyReady || timingReady || paceEscalation)) {
            return label;
        }
        if (paceEscalation && strategyReady && (timingReady || paceAccelerating)) {
            return label;
        }
        return SuggestionLabel.MONITOR;
    }

    private SuggestionLabel applyAdaptiveRegimeActionabilityGate(
            SuggestionLabel label,
            double totalScore,
            LapEvent current,
            String trackStatus,
            double signalConfidence) {
        if (!isActionableLabel(label)) {
            return label;
        }

        if (signalConfidence < MIN_CONFIDENCE_FOR_ACTIONABLE) {
            return SuggestionLabel.MONITOR;
        }

        double requiredScore = adaptiveActionThreshold(current, trackStatus, signalConfidence);
        if (totalScore < requiredScore) {
            return SuggestionLabel.MONITOR;
        }

        return label;
    }

    private static TimingPressureInfo resolveTimingPressureInfo(
            DriverPitState state,
            double urgencyScore,
            double strategyPenalty,
            double trafficScore,
            int trackStatusScore,
            double rivalPitReactionBoost) {
        if (state == null) {
            return new TimingPressureInfo(false, 0.0);
        }
        return new TimingPressureInfo(
                true,
                computeTimingPressureScore(
                        state,
                        urgencyScore,
                        strategyPenalty,
                        trafficScore,
                        trackStatusScore,
                        rivalPitReactionBoost));
    }

    private static TimingGateDecision applyDeterministicTimingGate(
            SuggestionLabel label,
            double totalScore,
            double urgencyScore,
            int tyreLife,
            String compound,
            int lapNumber,
            int totalLaps,
            String trackStatus,
            boolean timingPressureAvailable,
            double timingPressureScore) {
        if (label == SuggestionLabel.LOST_CHANCE) {
            return passthroughTimingDecision(label, "LOST_CHANCE_EMISSION");
        }
        if (label == SuggestionLabel.MONITOR) {
            return passthroughTimingDecision(label, "MONITOR_BELOW_ACTIONABLE");
        }
        if (label == SuggestionLabel.GOOD_PIT) {
            return new TimingGateDecision(
                    label,
                    SuggestionLabel.GOOD_PIT,
                    SemanticLabel.OPPORTUNITY,
                    false,
                    "ALREADY_OPPORTUNITY",
                    "OPPORTUNITY_BASELINE",
                    PitWindowPhase.UNKNOWN,
                    false,
                    false);
        }

        int expectedMaxStint = Math.max(1, defaultMaxStint(compound));
        double tireLifeRatio = (double) tyreLife / (double) expectedMaxStint;
        Double raceProgressPct = null;
        if (totalLaps > 0) {
            raceProgressPct = Math.max(0.0, Math.min(1.5, (double) lapNumber / (double) totalLaps));
        }

        PitWindowPhase pitWindowPhase = resolvePitWindowPhase(raceProgressPct, tireLifeRatio);
        boolean weakTimingCombo = urgencyScore < WEAK_URGENCY_LT && tyreLife < LOW_TYRE_LIFE_LT;
        boolean earlyWindow = raceProgressPct != null
                && raceProgressPct < EARLY_PROGRESS_LT
                && tireLifeRatio < EARLY_TIRE_LIFE_RATIO_LT;
        boolean cautionRegime = TrackStatusCodes.isCaution(trackStatus);

        if (cautionRegime) {
            boolean cautionTimingEvidence = (timingPressureAvailable && timingPressureScore >= CAUTION_MIN_TIMING_PRESSURE)
                    || (urgencyScore >= WEAK_URGENCY_LT && tireLifeRatio >= CAUTION_MIN_TIRE_LIFE_RATIO)
                    || (totalScore >= CAUTION_SCORE_HARD_FLOOR && !weakTimingCombo && !earlyWindow);
            if (!cautionTimingEvidence) {
                return new TimingGateDecision(
                        label,
                        SuggestionLabel.GOOD_PIT,
                        SemanticLabel.OPPORTUNITY,
                        false,
                        timingPressureAvailable ? "CAUTION_TIMING_WEAK" : "TIMING_PRESSURE_UNAVAILABLE",
                        "CAUTION_GUARD_DOWNGRADE",
                        pitWindowPhase,
                        weakTimingCombo,
                        earlyWindow);
            }
            return new TimingGateDecision(
                    label,
                    SuggestionLabel.PIT_NOW,
                    SemanticLabel.PIT_NOW,
                    true,
                    "CAUTION_TIMING_CONFIRMED",
                    "TIMING_GATE_PASS",
                    pitWindowPhase,
                    weakTimingCombo,
                    earlyWindow);
        }

        if (weakTimingCombo) {
            return new TimingGateDecision(
                    label,
                    SuggestionLabel.GOOD_PIT,
                    SemanticLabel.OPPORTUNITY,
                    false,
                    "WEAK_URGENCY_AND_LOW_TYRELIFE",
                    "WEAK_TIMING_SUPPRESSION",
                    pitWindowPhase,
                    true,
                    earlyWindow);
        }

        if (pitWindowPhase != PitWindowPhase.UNKNOWN
                && earlyWindow
                && tireLifeRatio < OVERDUE_TIRE_RATIO_OVERRIDE) {
            return new TimingGateDecision(
                    label,
                    SuggestionLabel.GOOD_PIT,
                    SemanticLabel.OPPORTUNITY,
                    false,
                    "EARLY_PROGRESS_WINDOW",
                    "EARLY_WINDOW_SUPPRESSION",
                    pitWindowPhase,
                    false,
                    true);
        }

        boolean baseTimingEvidence = (timingPressureAvailable && timingPressureScore >= EARLY_ACTION_MIN_TIMING_PRESSURE)
                || urgencyScore >= WEAK_URGENCY_LT
                || tireLifeRatio >= PIT_NOW_MIN_TIRE_LIFE_RATIO;

        if (!baseTimingEvidence) {
            String gateReason = timingPressureAvailable ? "INSUFFICIENT_TIMING_EVIDENCE" : "TIMING_PRESSURE_UNAVAILABLE";
            return new TimingGateDecision(
                    label,
                    SuggestionLabel.GOOD_PIT,
                    SemanticLabel.OPPORTUNITY,
                    false,
                    gateReason,
                    "TIMING_EVIDENCE_DOWNGRADE",
                    pitWindowPhase,
                    false,
                    false);
        }

        return new TimingGateDecision(
                label,
                SuggestionLabel.PIT_NOW,
                SemanticLabel.PIT_NOW,
                true,
                "TIMING_EVIDENCE_CONFIRMED",
                "TIMING_GATE_PASS",
                pitWindowPhase,
                false,
                false);
    }

    private static PitWindowPhase resolvePitWindowPhase(Double raceProgressPct, double tireLifeRatio) {
        if (raceProgressPct == null) {
            return PitWindowPhase.UNKNOWN;
        }
        if (raceProgressPct < EARLY_PROGRESS_LT && tireLifeRatio < OVERDUE_TIRE_RATIO_OVERRIDE) {
            return PitWindowPhase.TOO_EARLY;
        }
        if (raceProgressPct >= 0.95 || tireLifeRatio >= 1.30) {
            return PitWindowPhase.OVERDUE;
        }
        if (raceProgressPct >= 0.80 || tireLifeRatio >= 1.05) {
            return PitWindowPhase.LATE;
        }
        return PitWindowPhase.WINDOW_OPEN;
    }

    private static TimingGateDecision passthroughTimingDecision(SuggestionLabel label, String reason) {
        SemanticLabel semanticLabel = switch (label) {
            case PIT_NOW -> SemanticLabel.PIT_NOW;
            case GOOD_PIT -> SemanticLabel.OPPORTUNITY;
            case LOST_CHANCE -> SemanticLabel.LOST_CHANCE;
            default -> SemanticLabel.MONITOR;
        };
        return new TimingGateDecision(
                label,
                label,
                semanticLabel,
                label == SuggestionLabel.PIT_NOW,
                "NO_TIMING_GATE",
                reason,
                PitWindowPhase.UNKNOWN,
                false,
                false);
    }

    private static TimingGateDecision toPriorPromotedDecision(TimingGateDecision base) {
        return new TimingGateDecision(
                base.originalLabel,
                SuggestionLabel.PIT_NOW,
                SemanticLabel.PIT_NOW,
                true,
                "PRIOR_WINDOW_PROMOTION_STRICT",
                "PRIOR_WINDOW_PROMOTION_STRICT",
                base.pitWindowPhase,
                base.weakTimingCombo,
                base.earlyWindow);
    }

    private static TimingGateDecision toRivalPromotedDecision(TimingGateDecision base) {
        return new TimingGateDecision(
                base.originalLabel,
                SuggestionLabel.PIT_NOW,
                SemanticLabel.PIT_NOW,
                true,
                "RIVAL_RECENT_PIT_PROMOTION",
                "RIVAL_RECENT_PIT_PROMOTION",
                base.pitWindowPhase,
                base.weakTimingCombo,
                base.earlyWindow);
    }

    private static String readStringSetting(String key, String defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            raw = System.getenv(key);
        }
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return raw.trim();
    }

    private static boolean readBooleanSetting(String key, boolean defaultValue) {
        String raw = readStringSetting(key, defaultValue ? "true" : "false");
        if (raw == null) {
            return defaultValue;
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        if ("1".equals(normalized) || "true".equals(normalized) || "yes".equals(normalized) || "on".equals(normalized)) {
            return true;
        }
        if ("0".equals(normalized) || "false".equals(normalized) || "no".equals(normalized) || "off".equals(normalized)) {
            return false;
        }
        return defaultValue;
    }

    private static int readIntSetting(String key, int defaultValue) {
        String raw = readStringSetting(key, Integer.toString(defaultValue));
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ignore) {
            return defaultValue;
        }
    }

    private static double readDoubleSetting(String key, double defaultValue) {
        String raw = readStringSetting(key, Double.toString(defaultValue));
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException ignore) {
            return defaultValue;
        }
    }

    private static String normalizeRaceNameForPrior(String race) {
        if (race == null) {
            return "";
        }
        String trimmed = race.trim();
        return trimmed.replaceFirst("^\\d{4}\\s::\\s", "").trim().toLowerCase(Locale.ROOT);
    }

    private static String normalizeCompoundForPrior(String compound) {
        if (compound == null) {
            return "UNKNOWN";
        }
        String up = compound.trim().toUpperCase(Locale.ROOT);
        return up.isEmpty() ? "UNKNOWN" : up;
    }

    private static Double jsonDoubleOrNull(JsonNode node, String field) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return null;
        }
        JsonNode value = node.get(field);
        if (!value.isNumber()) {
            return null;
        }
        double v = value.asDouble();
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return null;
        }
        return v;
    }

    private static int minSamplesForKeyType(String keyType) {
        if ("race_compound_stint".equals(keyType)) {
            return PRIOR_MIN_SAMPLES_RACE_COMPOUND_STINT;
        }
        if ("race_stint".equals(keyType)) {
            return PRIOR_MIN_SAMPLES_RACE_STINT;
        }
        if ("compound_stint".equals(keyType)) {
            return PRIOR_MIN_SAMPLES_COMPOUND_STINT;
        }
        return PRIOR_MIN_SAMPLES_GLOBAL_STINT;
    }

    private static String composePriorKey(String keyType, String raceNameNormalized, String compound, int stint) {
        if ("race_compound_stint".equals(keyType)) {
            return keyType + "|" + raceNameNormalized + "|" + compound + "|" + stint;
        }
        if ("race_stint".equals(keyType)) {
            return keyType + "|" + raceNameNormalized + "|" + stint;
        }
        if ("compound_stint".equals(keyType)) {
            return keyType + "|" + compound + "|" + stint;
        }
        return "global_stint|" + stint;
    }

    private static PriorWindowPhase resolvePriorWindowPhase(PriorStats prior, Double raceProgressPct, int tyreLife) {
        if (prior == null) {
            return PriorWindowPhase.UNKNOWN;
        }
        if (raceProgressPct != null && prior.progressQ25 != null && prior.progressQ75 != null) {
            if (raceProgressPct < prior.progressQ25) {
                return PriorWindowPhase.TOO_EARLY;
            }
            if (raceProgressPct <= prior.progressQ75) {
                return PriorWindowPhase.WINDOW_OPEN;
            }
            if (prior.progressQ90 != null && raceProgressPct <= prior.progressQ90) {
                return PriorWindowPhase.LATE_WINDOW;
            }
            return PriorWindowPhase.OVERDUE;
        }
        if (prior.tyreQ25 != null && prior.tyreQ75 != null) {
            if (tyreLife < prior.tyreQ25) {
                return PriorWindowPhase.TOO_EARLY;
            }
            if (tyreLife <= prior.tyreQ75) {
                return PriorWindowPhase.WINDOW_OPEN;
            }
            if (prior.tyreQ90 != null && tyreLife <= prior.tyreQ90) {
                return PriorWindowPhase.LATE_WINDOW;
            }
            return PriorWindowPhase.OVERDUE;
        }
        return PriorWindowPhase.UNKNOWN;
    }

    private static PriorMatch resolvePriorMatch(
            Map<String, PriorStats> priorMap,
            String raceNameNormalized,
            String compound,
            int stint) {
        if (priorMap == null || priorMap.isEmpty()) {
            return null;
        }

        String[] keyTypes = new String[] {"race_compound_stint", "race_stint", "compound_stint", "global_stint"};
        String[] fallbackLevels = new String[] {
            "race_compound_stint",
            "race_stint",
            "compound_stint",
            "global_stint"
        };

        for (int i = 0; i < keyTypes.length; i++) {
            String keyType = keyTypes[i];
            String key = composePriorKey(keyType, raceNameNormalized, compound, stint);
            PriorStats prior = priorMap.get(key);
            if (prior == null) {
                continue;
            }
            if (prior.sampleCount < minSamplesForKeyType(keyType)) {
                continue;
            }
            return new PriorMatch(prior, key, fallbackLevels[i]);
        }
        return null;
    }

    private static PriorPromotionDecision evaluatePriorPromotionDecision(
            boolean priorPromotionEnabled,
            boolean priorPromotionStrictMode,
            boolean priorsAvailable,
            Map<String, PriorStats> priorMap,
            TimingGateDecision timingDecision,
            LapEvent current,
            String trackStatus,
            double urgencyScore,
            double totalScore,
            boolean timingPressureAvailable,
            double timingPressureScore) {
        if (!priorPromotionEnabled) {
            return PriorPromotionDecision.unavailable("PRIOR_PROMOTION_DISABLED");
        }
        if (!priorsAvailable) {
            return PriorPromotionDecision.unavailable("PRIORS_UNAVAILABLE");
        }
        if (timingDecision == null || timingDecision.legacyLabel != SuggestionLabel.GOOD_PIT) {
            return PriorPromotionDecision.unavailable("NOT_OPPORTUNITY_LABEL");
        }

        if (PRIOR_PROMOTION_GREEN_ONLY && TrackStatusCodes.isCaution(trackStatus)) {
            return new PriorPromotionDecision(
                    false,
                    "CAUTION_SKIPPED",
                    PRIOR_PROMOTION_CAUTION_SKIP_REASON,
                    "",
                    "",
                    "UNKNOWN",
                    0,
                    PriorWindowPhase.UNKNOWN,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
        }

        String raceNameNormalized = normalizeRaceNameForPrior(current.getRace());
        String compound = normalizeCompoundForPrior(current.getCompound());
        int stint = Math.max(1, current.getStint());
        PriorMatch match = resolvePriorMatch(priorMap, raceNameNormalized, compound, stint);
        if (match == null) {
            return PriorPromotionDecision.unavailable("NO_PRIOR_MATCH");
        }

        int expectedMaxStint = Math.max(1, defaultMaxStint(current.getCompound()));
        double tireLifeRatio = (double) current.getTyreLife() / (double) expectedMaxStint;
        Double raceProgressPct = null;
        if (current.getTotalLaps() > 0) {
            raceProgressPct = Math.max(0.0, Math.min(1.5, (double) current.getLapNumber() / (double) current.getTotalLaps()));
        }
        PriorWindowPhase priorPhase = resolvePriorWindowPhase(match.prior, raceProgressPct, current.getTyreLife());
        String fallbackLevel = nullToEmpty(match.fallbackLevel);
        String priorConfidence = nullToEmpty(match.prior.priorConfidence).toUpperCase(Locale.ROOT);

        if (priorPromotionStrictMode) {
            boolean keyAllowed = "race_compound_stint".equals(fallbackLevel) || "race_stint".equals(fallbackLevel);
            if (!keyAllowed) {
                return new PriorPromotionDecision(
                        false,
                        "PRIOR_REJECT_FALLBACK_LEVEL",
                        "",
                        match.priorKeyUsed,
                        fallbackLevel,
                        priorConfidence,
                        match.prior.sampleCount,
                        priorPhase,
                        match.prior.progressQ25,
                        match.prior.progressQ50,
                        match.prior.progressQ75,
                        match.prior.progressQ90,
                        match.prior.tyreQ25,
                        match.prior.tyreQ50,
                        match.prior.tyreQ75,
                        match.prior.tyreQ90);
            }
            boolean confidenceAllowed = "HIGH".equals(priorConfidence) || "MEDIUM".equals(priorConfidence);
            if (!confidenceAllowed) {
                return new PriorPromotionDecision(
                        false,
                        "PRIOR_REJECT_LOW_CONFIDENCE",
                        "",
                        match.priorKeyUsed,
                        fallbackLevel,
                        priorConfidence,
                        match.prior.sampleCount,
                        priorPhase,
                        match.prior.progressQ25,
                        match.prior.progressQ50,
                        match.prior.progressQ75,
                        match.prior.progressQ90,
                        match.prior.tyreQ25,
                        match.prior.tyreQ50,
                        match.prior.tyreQ75,
                        match.prior.tyreQ90);
            }
        }

        boolean inProgressWindow = raceProgressPct != null
                && match.prior.progressQ25 != null
                && match.prior.progressQ75 != null
                && raceProgressPct >= match.prior.progressQ25
                && raceProgressPct <= match.prior.progressQ75;
        boolean tyreInMidWindow = match.prior.tyreQ50 != null
                && match.prior.tyreQ90 != null
                && current.getTyreLife() >= match.prior.tyreQ50
                && current.getTyreLife() <= match.prior.tyreQ90;
        boolean tyreAtLeastQ50 = match.prior.tyreQ50 != null && current.getTyreLife() >= match.prior.tyreQ50;
        boolean tyreAtLeastQ75 = match.prior.tyreQ75 != null && current.getTyreLife() >= match.prior.tyreQ75;
        boolean strongTiming = timingPressureAvailable && timingPressureScore >= PRIOR_STRICT_MIN_TIMING_PRESSURE;
        boolean urgencyStrong = urgencyScore >= WEAK_URGENCY_LT;
        boolean ratioAtLeast80 = tireLifeRatio >= PRIOR_STRICT_MIN_TIRE_LIFE_RATIO;
        boolean ratioAtLeast85 = tireLifeRatio >= PRIOR_STRICT_MIN_TIRE_LIFE_RATIO_LATE;

        int passCount = 0;
        if (urgencyStrong) {
            passCount++;
        }
        if (strongTiming) {
            passCount++;
        }
        if (tyreAtLeastQ50) {
            passCount++;
        }
        if (ratioAtLeast80) {
            passCount++;
        }
        if (inProgressWindow) {
            passCount++;
        }

        boolean applyPromotion = false;
        String reason;

        if (priorPhase == PriorWindowPhase.WINDOW_OPEN) {
            applyPromotion = passCount >= PRIOR_STRICT_WINDOW_OPEN_MIN_PASS
                    && (inProgressWindow || (tyreInMidWindow && urgencyStrong))
                    && (ratioAtLeast80 || strongTiming);
            if (applyPromotion) {
                reason = "PRIOR_WINDOW_PROMOTION_STRICT";
            } else if (!ratioAtLeast80 && !strongTiming) {
                reason = "PRIOR_REJECT_LOW_TYRE_RATIO";
            } else {
                reason = "PRIOR_REJECT_WEAK_EVIDENCE";
            }
        } else if (priorPhase == PriorWindowPhase.LATE_WINDOW) {
            boolean lateStrong = totalScore >= PRIOR_STRICT_MIN_TOTAL_SCORE_LATE
                    && urgencyStrong
                    && strongTiming
                    && (ratioAtLeast85 || tyreAtLeastQ75);
            applyPromotion = lateStrong;
            reason = lateStrong ? "PRIOR_WINDOW_PROMOTION_STRICT" : "PRIOR_REJECT_WEAK_EVIDENCE";
        } else if (priorPhase == PriorWindowPhase.TOO_EARLY) {
            reason = "PRIOR_REJECT_TOO_EARLY";
        } else if (priorPhase == PriorWindowPhase.UNKNOWN) {
            reason = "PRIOR_REJECT_UNKNOWN_PHASE";
        } else {
            reason = "PRIOR_REJECT_WEAK_EVIDENCE";
        }

        return new PriorPromotionDecision(
                applyPromotion,
                reason,
                "",
                match.priorKeyUsed,
                fallbackLevel,
                priorConfidence,
                match.prior.sampleCount,
                priorPhase,
                match.prior.progressQ25,
                match.prior.progressQ50,
                match.prior.progressQ75,
                match.prior.progressQ90,
                match.prior.tyreQ25,
                match.prior.tyreQ50,
                match.prior.tyreQ75,
                match.prior.tyreQ90);
    }

    private static int recentPitWithinNLaps(MapState<String, Integer> lastPitState, String driver, int currentLap, int maxWindow)
            throws Exception {
        if (lastPitState == null || driver == null || driver.isBlank()) {
            return -1;
        }
        Integer pitLap = lastPitState.get(driver);
        if (pitLap == null) {
            return -1;
        }
        int delta = currentLap - pitLap;
        if (delta < 0 || delta > maxWindow) {
            return -1;
        }
        return delta;
    }

    private static boolean validGap(Double gap) {
        if (gap == null) {
            return false;
        }
        if (Double.isNaN(gap) || Double.isInfinite(gap)) {
            return false;
        }
        if (gap <= 0.0) {
            return false;
        }
        return gap <= 20.0;
    }

    private RivalPromotionDecision evaluateRivalPromotionDecision(
            boolean rivalPressureEnabled,
            boolean rivalPressureCautionEnabled,
            TimingGateDecision timingDecision,
            LapEvent current,
            List<LapEvent> laps,
            int posIndex,
            String trackStatus,
            double urgencyScore,
            boolean timingPressureAvailable,
            double timingPressureScore) throws Exception {
        if (!rivalPressureEnabled) {
            return RivalPromotionDecision.unavailable("RIVAL_PROMOTION_DISABLED");
        }
        if (timingDecision == null || timingDecision.legacyLabel != SuggestionLabel.GOOD_PIT) {
            return RivalPromotionDecision.unavailable("NOT_OPPORTUNITY_LABEL");
        }
        boolean caution = TrackStatusCodes.isCaution(trackStatus);
        if (caution && !rivalPressureCautionEnabled) {
            return new RivalPromotionDecision(
                    false,
                    "CAUTION_PROMOTION_DISABLED",
                    "",
                    "",
                    "",
                    null,
                    null,
                    -1,
                    -1,
                    -1);
        }
        if (timingDecision.pitWindowPhase == PitWindowPhase.TOO_EARLY) {
            return new RivalPromotionDecision(
                    false,
                    "RIVAL_REJECT_TOO_EARLY",
                    "",
                    "",
                    "",
                    null,
                    null,
                    -1,
                    -1,
                    -1);
        }

        LapEvent ahead = posIndex > 0 ? laps.get(posIndex - 1) : null;
        LapEvent behind = (posIndex + 1) < laps.size() ? laps.get(posIndex + 1) : null;
        String aheadDriver = ahead != null ? nullToEmpty(ahead.getDriver()) : "";
        String behindDriver = behind != null ? nullToEmpty(behind.getDriver()) : "";
        Double gapAhead = current.getGapToCarAhead();
        Double gapBehind = behind != null ? behind.getGapToCarAhead() : null;

        int aheadRecent = recentPitWithinNLaps(lastObservedPitLap, aheadDriver, current.getLapNumber(), RIVAL_RECENT_PIT_WINDOW_LONG);
        int behindRecent = recentPitWithinNLaps(lastObservedPitLap, behindDriver, current.getLapNumber(), RIVAL_RECENT_PIT_WINDOW_LONG);
        int teammateRecent = -1;

        String team = nullToEmpty(current.getTeam());
        if (!team.isBlank()) {
            for (LapEvent candidate : laps) {
                if (candidate == null) {
                    continue;
                }
                if (candidate.getDriver() == null || candidate.getDriver().equals(current.getDriver())) {
                    continue;
                }
                if (!team.equalsIgnoreCase(nullToEmpty(candidate.getTeam()))) {
                    continue;
                }
                teammateRecent = recentPitWithinNLaps(
                        lastObservedPitLap,
                        candidate.getDriver(),
                        current.getLapNumber(),
                        RIVAL_RECENT_PIT_WINDOW_LONG);
                if (teammateRecent >= 0) {
                    break;
                }
            }
        }

        int recentWindow = c6TunedRivalProfileEnabled ? rivalRecentMaxLaps : RIVAL_RECENT_PIT_WINDOW_SHORT;
        boolean hasRecentRivalEvent = (aheadRecent >= 0 && aheadRecent <= recentWindow)
                || (behindRecent >= 0 && behindRecent <= recentWindow)
                || (teammateRecent >= 0 && teammateRecent <= recentWindow);
        if (!hasRecentRivalEvent) {
            return new RivalPromotionDecision(
                    false,
                    "RIVAL_REJECT_NO_RECENT_PIT_EVENT",
                    "",
                    aheadDriver,
                    behindDriver,
                    gapAhead,
                    gapBehind,
                    aheadRecent,
                    behindRecent,
                    teammateRecent);
        }

        boolean gapAheadValid = validGap(gapAhead);
        boolean gapBehindValid = validGap(gapBehind);
        boolean gapCondition;
        String source;
        if (caution) {
            gapCondition = true; // caution uses event-only by design.
            source = "EVENT_ONLY_CAUTION";
        } else {
            boolean aheadGapShort = gapAheadValid && gapAhead <= RIVAL_PROMOTION_MAX_GAP_SEC_GREEN;
            boolean behindGapShort = gapBehindValid && gapBehind <= RIVAL_PROMOTION_MAX_GAP_SEC_GREEN;
            boolean aheadGapRelaxed = gapAheadValid && gapAhead <= RIVAL_PROMOTION_MAX_GAP_SEC_GREEN_RELAXED;
            boolean behindGapRelaxed = gapBehindValid && gapBehind <= RIVAL_PROMOTION_MAX_GAP_SEC_GREEN_RELAXED;
            gapCondition = aheadGapShort || behindGapShort
                    || ((aheadRecent == 0 || behindRecent == 0) && (aheadGapRelaxed || behindGapRelaxed));
            source = gapCondition ? "CLASSIFICATION_NEIGHBOR_GAP" : "INVALID_OR_UNAVAILABLE";
        }

        if (!gapCondition) {
            return new RivalPromotionDecision(
                    false,
                    "RIVAL_REJECT_INVALID_GAP",
                    source,
                    aheadDriver,
                    behindDriver,
                    gapAhead,
                    gapBehind,
                    aheadRecent,
                    behindRecent,
                    teammateRecent);
        }

        int expectedMaxStint = Math.max(1, defaultMaxStint(current.getCompound()));
        double tireLifeRatio = (double) current.getTyreLife() / (double) expectedMaxStint;
        boolean timingEvidence = urgencyScore >= WEAK_URGENCY_LT
                || tireLifeRatio >= PRIOR_STRICT_MIN_TIRE_LIFE_RATIO
                || (timingPressureAvailable && timingPressureScore >= RIVAL_PROMOTION_MIN_TIMING_PRESSURE);
        if (!timingEvidence) {
            return new RivalPromotionDecision(
                    false,
                    "RIVAL_REJECT_WEAK_TIMING_EVIDENCE",
                    source,
                    aheadDriver,
                    behindDriver,
                    gapAhead,
                    gapBehind,
                    aheadRecent,
                    behindRecent,
                    teammateRecent);
        }

        boolean ultraCloseGuardApplied = false;
        if (c6TunedRivalProfileEnabled) {
            C6RivalFilterDecision c6Filter = evaluateC6TunedRivalFilter(
                    rivalRecentMaxLaps,
                    rivalMinUrgency,
                    rivalMinTimingPressure,
                    rivalUltraCloseGapGuardEnabled,
                    caution,
                    aheadRecent,
                    behindRecent,
                    teammateRecent,
                    urgencyScore,
                    timingPressureAvailable,
                    timingPressureScore,
                    gapAhead,
                    gapBehind,
                    tireLifeRatio);
            ultraCloseGuardApplied = c6Filter.ultraCloseGuardApplied;
            if (!c6Filter.pass) {
                return new RivalPromotionDecision(
                        false,
                        c6Filter.reason,
                        source,
                        aheadDriver,
                        behindDriver,
                        gapAhead,
                        gapBehind,
                        aheadRecent,
                        behindRecent,
                        teammateRecent,
                        ultraCloseGuardApplied);
            }
        }

        return new RivalPromotionDecision(
                true,
                "RIVAL_RECENT_PIT_PROMOTION",
                source,
                aheadDriver,
                behindDriver,
                gapAhead,
                gapBehind,
                aheadRecent,
                behindRecent,
                teammateRecent,
                ultraCloseGuardApplied);
    }

    private static C6RivalFilterDecision evaluateC6TunedRivalFilter(
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
            double tireLifeRatio) {
        boolean hasRecentRivalEvent = (aheadRecent >= 0 && aheadRecent <= rivalRecentMaxLaps)
                || (behindRecent >= 0 && behindRecent <= rivalRecentMaxLaps)
                || (teammateRecent >= 0 && teammateRecent <= rivalRecentMaxLaps);
        if (!hasRecentRivalEvent) {
            return new C6RivalFilterDecision(false, "C6_REJECT_RECENCY", false);
        }

        if (urgencyScore < rivalMinUrgency) {
            return new C6RivalFilterDecision(false, "C6_REJECT_URGENCY", false);
        }

        if (!timingPressureAvailable) {
            return new C6RivalFilterDecision(false, "C6_REJECT_TIMING_PRESSURE_UNAVAILABLE", false);
        }
        if (timingPressureScore < rivalMinTimingPressure) {
            return new C6RivalFilterDecision(false, "C6_REJECT_TIMING_PRESSURE", false);
        }

        if (rivalUltraCloseGapGuardEnabled && !cautionRegime) {
            Double minGap = null;
            if (validGap(gapAhead)) {
                minGap = gapAhead;
            }
            if (validGap(gapBehind)) {
                minGap = minGap == null ? gapBehind : Math.min(minGap, gapBehind);
            }
            if (minGap != null && minGap <= C6_ULTRA_CLOSE_GAP_SEC) {
                boolean extraEvidence = (urgencyScore >= 10.0 && urgencyScore <= 20.0)
                        || tireLifeRatio >= C6_ULTRA_CLOSE_RATIO_OVERRIDE;
                if (!extraEvidence) {
                    return new C6RivalFilterDecision(false, "C6_REJECT_ULTRA_CLOSE_GUARD", true);
                }
                return new C6RivalFilterDecision(true, "C6_PROFILE_PASS", true);
            }
        }

        return new C6RivalFilterDecision(true, "C6_PROFILE_PASS", false);
    }

    private void loadPitWindowPriors() {
        pitWindowPriors = new HashMap<>();
        pitWindowPriorsAvailable = false;
        pitWindowPriorsStatus = "PRIORS_UNAVAILABLE";
        pitWindowPriorsLoadedAt = "";

        boolean enabled = readBooleanSetting(PRIORS_ENABLED_SETTING, true);
        if (!enabled) {
            pitWindowPriorsStatus = "PRIORS_DISABLED";
            pitWindowPriorsPath = readStringSetting(PRIORS_PATH_SETTING, DEFAULT_PRIORS_PATH);
            return;
        }

        pitWindowPriorsPath = readStringSetting(PRIORS_PATH_SETTING, DEFAULT_PRIORS_PATH);
        Path path = Paths.get(pitWindowPriorsPath);
        if (!Files.exists(path)) {
            pitWindowPriorsStatus = "PRIORS_FILE_MISSING";
            return;
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(Files.readString(path));
            JsonNode priorsNode = root.path("priors");
            if (!priorsNode.isObject()) {
                pitWindowPriorsStatus = "PRIORS_SCHEMA_INVALID";
                return;
            }

            Map<String, PriorStats> loaded = new HashMap<>();
            var fields = priorsNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String key = entry.getKey();
                JsonNode node = entry.getValue();
                String keyType = node.path("key_type").asText("");
                int sampleCount = node.path("sample_count").asInt(0);
                String priorConfidence = node.path("prior_confidence").asText("UNKNOWN");

                PriorStats stats = new PriorStats(
                        keyType,
                        key,
                        sampleCount,
                        priorConfidence,
                        jsonDoubleOrNull(node, "race_progress_pct_q25"),
                        jsonDoubleOrNull(node, "race_progress_pct_q50"),
                        jsonDoubleOrNull(node, "race_progress_pct_q75"),
                        jsonDoubleOrNull(node, "race_progress_pct_q90"),
                        jsonDoubleOrNull(node, "tyreLife_q25"),
                        jsonDoubleOrNull(node, "tyreLife_q50"),
                        jsonDoubleOrNull(node, "tyreLife_q75"),
                        jsonDoubleOrNull(node, "tyreLife_q90"));
                loaded.put(key, stats);
            }

            pitWindowPriors = loaded;
            pitWindowPriorsAvailable = !pitWindowPriors.isEmpty();
            pitWindowPriorsStatus = pitWindowPriorsAvailable ? "PRIORS_LOADED" : "PRIORS_EMPTY";
            pitWindowPriorsLoadedAt = Instant.now().toString();
            LOG.info("Loaded pit-window priors: {} entries from {}", pitWindowPriors.size(), pitWindowPriorsPath);
        } catch (Exception e) { // NOPMD deliberate broad catch for fail-open behavior
            pitWindowPriorsAvailable = false;
            pitWindowPriorsStatus = "PRIORS_UNREADABLE";
            LOG.warn("Failed to load pit-window priors from {}: {}", pitWindowPriorsPath, e.toString());
        }
    }

    private String buildDecisionMetadataJson(
            LapEvent current,
            double totalScore,
            double urgencyScore,
            double timingPressureScore,
            TimingGateDecision decision,
            PriorPromotionDecision priorDecision,
            RivalPromotionDecision rivalDecision) {
        int expectedMaxStint = Math.max(1, defaultMaxStint(current.getCompound()));
        double tireLifeRatio = (double) current.getTyreLife() / (double) expectedMaxStint;
        Double raceProgressPct = null;
        if (current.getTotalLaps() > 0) {
            raceProgressPct = Math.max(0.0, Math.min(1.5,
                    (double) current.getLapNumber() / (double) current.getTotalLaps()));
        }
        String raceProgressLiteral = raceProgressPct == null
                ? "null"
                : String.format("%.6f", raceProgressPct);
        return "{"
                + "\"originalLabel\":\"" + jsonEscape(decision.originalLabel.name()) + "\","
                + "\"finalLabel\":\"" + jsonEscape(decision.legacyLabel.name()) + "\","
                + "\"semanticLabel\":\"" + jsonEscape(decision.semanticLabel.name()) + "\","
                + "\"timingGatePassed\":" + decision.timingGatePassed + ","
                + "\"timingGateReason\":\"" + jsonEscape(decision.timingGateReason) + "\","
                + "\"pitWindowPhase\":\"" + jsonEscape(decision.pitWindowPhase.name()) + "\","
                + "\"finalDecisionReason\":\"" + jsonEscape(decision.finalDecisionReason) + "\","
                + "\"totalScore\":" + String.format("%.6f", totalScore) + ","
                + "\"urgencyScore\":" + String.format("%.6f", urgencyScore) + ","
                + "\"timingPressureScore\":" + String.format("%.6f", timingPressureScore) + ","
                + "\"tyreLife\":" + current.getTyreLife() + ","
                + "\"expectedMaxStint\":" + expectedMaxStint + ","
                + "\"tireLifeRatio\":" + String.format("%.6f", tireLifeRatio) + ","
                + "\"raceProgressPct\":" + raceProgressLiteral + ","
                + "\"trackStatus\":\"" + jsonEscape(TrackStatusCodes.normalizeOrGreen(current.getTrackStatus())) + "\","
                + "\"regime\":\"" + (TrackStatusCodes.isCaution(current.getTrackStatus()) ? "CAUTION" : "GREEN") + "\","
                + "\"weakTimingCombo\":" + decision.weakTimingCombo + ","
                + "\"earlyWindow\":" + decision.earlyWindow + ","
                + "\"priorPromotionApplied\":" + priorDecision.priorPromotionApplied + ","
                + "\"priorPromotionReason\":\"" + jsonEscape(priorDecision.priorPromotionReason) + "\","
                + "\"priorPromotionSkippedReason\":\"" + jsonEscape(priorDecision.priorPromotionSkippedReason) + "\","
                + "\"priorKeyUsed\":\"" + jsonEscape(priorDecision.priorKeyUsed) + "\","
                + "\"fallbackLevel\":\"" + jsonEscape(priorDecision.fallbackLevel) + "\","
                + "\"priorSampleCount\":" + priorDecision.priorSampleCount + ","
                + "\"priorConfidence\":\"" + jsonEscape(priorDecision.priorConfidence) + "\","
                + "\"priorWindowPhase\":\"" + jsonEscape(priorDecision.priorWindowPhase.name()) + "\","
                + "\"priorProgressQ25\":" + jsonLiteral(priorDecision.priorProgressQ25) + ","
                + "\"priorProgressQ50\":" + jsonLiteral(priorDecision.priorProgressQ50) + ","
                + "\"priorProgressQ75\":" + jsonLiteral(priorDecision.priorProgressQ75) + ","
                + "\"priorProgressQ90\":" + jsonLiteral(priorDecision.priorProgressQ90) + ","
                + "\"priorTyreQ25\":" + jsonLiteral(priorDecision.priorTyreQ25) + ","
                + "\"priorTyreQ50\":" + jsonLiteral(priorDecision.priorTyreQ50) + ","
                + "\"priorTyreQ75\":" + jsonLiteral(priorDecision.priorTyreQ75) + ","
                + "\"priorTyreQ90\":" + jsonLiteral(priorDecision.priorTyreQ90) + ","
                + "\"rivalPressureApplied\":" + rivalDecision.rivalPressureApplied + ","
                + "\"rivalPressureReason\":\"" + jsonEscape(rivalDecision.rivalPressureReason) + "\","
                + "\"rivalPressureSource\":\"" + jsonEscape(rivalDecision.rivalPressureSource) + "\","
                + "\"classificationAheadDriver\":\"" + jsonEscape(rivalDecision.classificationAheadDriver) + "\","
                + "\"classificationBehindDriver\":\"" + jsonEscape(rivalDecision.classificationBehindDriver) + "\","
                + "\"classificationGapAheadSec\":" + jsonLiteral(rivalDecision.classificationGapAheadSec) + ","
                + "\"classificationGapBehindSec\":" + jsonLiteral(rivalDecision.classificationGapBehindSec) + ","
                + "\"aheadPittedLastNLaps\":" + rivalDecision.aheadPittedLastNLaps + ","
                + "\"behindPittedLastNLaps\":" + rivalDecision.behindPittedLastNLaps + ","
                + "\"teammatePittedLastNLaps\":" + rivalDecision.teammatePittedLastNLaps + ","
                + "\"rivalRecentMaxLaps\":" + rivalRecentMaxLaps + ","
                + "\"rivalMinUrgency\":" + String.format("%.6f", rivalMinUrgency) + ","
                + "\"rivalMinTimingPressure\":" + String.format("%.6f", rivalMinTimingPressure) + ","
                + "\"ultraCloseGuardApplied\":" + rivalDecision.ultraCloseGuardApplied + ","
                + "\"c6TunedProfileEnabled\":" + c6TunedRivalProfileEnabled + ","
                + "\"pitWindowPriorsStatus\":\"" + jsonEscape(nullToEmpty(pitWindowPriorsStatus)) + "\","
                + "\"pitWindowPriorsPath\":\"" + jsonEscape(nullToEmpty(pitWindowPriorsPath)) + "\","
                + "\"pitWindowPriorsLoadedAt\":\"" + jsonEscape(nullToEmpty(pitWindowPriorsLoadedAt)) + "\","
                + "\"featureFlagsActive\":\""
                + "priorPromotionEnabled=" + priorPromotionEnabled + ";"
                + "priorPromotionStrictMode=" + priorPromotionStrictMode + ";"
                + "rivalPressurePromotionEnabled=" + rivalPressurePromotionEnabled + ";"
                + "rivalPressureCautionEnabled=" + rivalPressureCautionEnabled + ";"
                + "priorSuppressionEnabled=" + priorSuppressionEnabled + ";"
                + "c6TunedRivalProfileEnabled=" + c6TunedRivalProfileEnabled + ";"
                + "rivalRecentMaxLaps=" + rivalRecentMaxLaps + ";"
                + "rivalMinUrgency=" + String.format("%.3f", rivalMinUrgency) + ";"
                + "rivalMinTimingPressure=" + String.format("%.3f", rivalMinTimingPressure) + ";"
                + "rivalUltraCloseGapGuardEnabled=" + rivalUltraCloseGapGuardEnabled
                + "\""
                + "}";
    }

    private static String jsonEscape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String jsonLiteral(Double value) {
        if (value == null || Double.isNaN(value) || Double.isInfinite(value)) {
            return "null";
        }
        return String.format("%.6f", value);
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static boolean isActionableLabel(SuggestionLabel label) {
        return label == SuggestionLabel.PIT_NOW || label == SuggestionLabel.GOOD_PIT;
    }

    private static boolean isPitNowLabel(SuggestionLabel label) {
        return label == SuggestionLabel.PIT_NOW;
    }

    private static double adaptiveActionThreshold(LapEvent current, String trackStatus, double signalConfidence) {
        double threshold;
        if (TrackStatusCodes.SAFETY_CAR.equals(trackStatus)) {
            threshold = 52.0;
        } else if (TrackStatusCodes.VIRTUAL_SAFETY_CAR.equals(trackStatus)
                || TrackStatusCodes.VSC_ENDING.equals(trackStatus)) {
            threshold = 56.0;
        } else {
            threshold = 60.0;
        }

        int totalLaps = Math.max(1, current.getTotalLaps());
        double raceProgress = Math.max(0.0, Math.min(1.5, (double) current.getLapNumber() / totalLaps));
        if (raceProgress >= 0.88) {
            threshold += 4.0;
        } else if (raceProgress >= 0.75) {
            threshold += 2.0;
        }

        if (signalConfidence < LOW_CONFIDENCE_THRESHOLD) {
            threshold += CONFIDENCE_PENALTY_ACTIONABLE;
        }

        return Math.max(50.0, Math.min(74.0, threshold));
    }

    private static double computeUncertaintyPenalty(double signalConfidence, String trackStatus) {
        double penalty = (1.0 - Math.max(0.0, Math.min(1.0, signalConfidence))) * MAX_CONFIDENCE_PENALTY;
        if (!TrackStatusCodes.isGreenOrUnknown(trackStatus)) {
            penalty = Math.max(0.0, penalty - 2.5);
        }
        return penalty;
    }

    private double computeCompetitivePressure(LapEvent current, List<LapEvent> laps, int posIndex) throws Exception {
        double pressure = 0.0;
        if (current.getGapToCarAhead() != null && current.getGapToCarAhead() >= 0.0) {
            pressure += 3.0 * Math.max(0.0, 1.0 - (current.getGapToCarAhead() / COMPETITIVE_GAP_REF));
        }

        if (posIndex + 1 < laps.size()) {
            LapEvent behind = laps.get(posIndex + 1);
            if (behind.getGapToCarAhead() != null && behind.getGapToCarAhead() >= 0.0) {
                pressure += 2.0 * Math.max(0.0, 1.0 - (behind.getGapToCarAhead() / COMPETITIVE_GAP_REF));
            }
            pressure += recentRivalPitComponent(current.getLapNumber(), behind.getDriver(), false);
        }

        if (posIndex > 0) {
            LapEvent ahead = laps.get(posIndex - 1);
            pressure += recentRivalPitComponent(current.getLapNumber(), ahead.getDriver(), true);
        }

        return Math.max(0.0, Math.min(MAX_COMPETITIVE_PRESSURE, pressure));
    }

    private double recentRivalPitComponent(int currentLap, String rivalDriver, boolean ahead) throws Exception {
        Integer rivalPitLap = lastObservedPitLap.get(rivalDriver);
        if (rivalPitLap == null) {
            return 0.0;
        }
        int lapDelta = currentLap - rivalPitLap;
        if (lapDelta < 0 || lapDelta > COMPETITIVE_RIVAL_PIT_WINDOW) {
            return 0.0;
        }
        double recencyWeight = 1.0 - ((double) lapDelta / (double) (COMPETITIVE_RIVAL_PIT_WINDOW + 1));
        return (ahead ? 3.0 : 2.0) * recencyWeight;
    }

    private static double computeSignalConfidence(
            LapEvent current,
            List<LapEvent> laps,
            int posIndex,
            DriverPitState state) {
        double confidence = 1.0;

        if (current.getGapToCarAhead() == null) {
            confidence -= 0.22;
        }
        if (current.getLapTime() == null || current.getLapTime() <= 0) {
            confidence -= 0.18;
        }
        if (current.getPitLoss() == null && current.getVscPitLoss() == null && current.getScPitLoss() == null) {
            confidence -= 0.20;
        }
        if (state == null || state.getConsecutiveSlowLaps() <= 0) {
            confidence -= 0.12;
        }

        int localContext = 1;
        if (posIndex > 0) {
            localContext++;
        }
        if (posIndex + 1 < laps.size()) {
            localContext++;
        }
        if (localContext < 3) {
            confidence -= 0.10;
        }

        return Math.max(0.0, Math.min(1.0, confidence));
    }

    private void closeDecisionEpisodeOnPit(LapEvent event) throws Exception {
        if (event.getPitInTime() == null) {
            return;
        }
        String driver = event.getDriver();
        lastObservedPitLap.put(driver, event.getLapNumber());
        if (activeEpisodeStartLap.contains(driver)) {
            activeEpisodeStartLap.remove(driver);
            episodeCloseReason.put(driver, "pit_in");
        }
    }

    private void closeExpiredDecisionEpisodes(int currentLap) throws Exception {
        List<String> toClose = new ArrayList<>();
        for (String driver : activeEpisodeStartLap.keys()) {
            Integer startLap = activeEpisodeStartLap.get(driver);
            if (startLap == null) {
                continue;
            }
            if (currentLap - startLap >= DECISION_EPISODE_HORIZON_LAPS) {
                toClose.add(driver);
            }
        }
        for (String driver : toClose) {
            activeEpisodeStartLap.remove(driver);
            episodeCloseReason.put(driver, "horizon_expiry");
        }
    }

    private boolean isDecisionEpisodeActive(String driver, int currentLap) throws Exception {
        Integer startLap = activeEpisodeStartLap.get(driver);
        if (startLap == null) {
            return false;
        }
        if (currentLap - startLap >= DECISION_EPISODE_HORIZON_LAPS) {
            activeEpisodeStartLap.remove(driver);
            episodeCloseReason.put(driver, "horizon_expiry");
            return false;
        }
        return true;
    }

    private void openDecisionEpisode(String driver, int lapNumber) throws Exception {
        activeEpisodeStartLap.put(driver, lapNumber);
        episodeCloseReason.put(driver, "active");
    }

    // decision-window pressure approximates whether the stop window is tightening now.
    // this follows timed strategy-window reasoning from carrasco 2023 and quiroga 2024,
    // using deterministic local trends (pace acceleration + gap expansion) only.
    private static double computeTimingPressureScore(
            DriverPitState state,
            double urgencyScore,
            double strategyPenalty,
            double trafficScore,
            int trackStatusScore,
            double rivalPitReactionBoost) {
        if (state == null || trackStatusScore > 0) {
            return 0.0;
        }
        if (urgencyScore < TIMING_PRESSURE_MIN_URGENCY && rivalPitReactionBoost <= 0.0) {
            return 0.0;
        }

        double paceAcceleration = Math.max(0.0, state.getPaceRatioDelta());
        double paceComponent = 6.0 * Math.min(1.0, paceAcceleration / TIMING_PRESSURE_PACE_ACCEL_REFERENCE);

        double gapExpansion = Math.max(0.0, state.getGapToCarAheadDelta());
        double gapComponent = 4.0 * Math.min(1.0, gapExpansion / TIMING_PRESSURE_GAP_EXPANSION_REFERENCE);

        double strategyComponent = strategyPenalty < 0.0
                ? 2.0 * Math.min(1.0, Math.abs(strategyPenalty) / 8.0)
                : 0.0;
        double rivalComponent = Math.min(2.5, rivalPitReactionBoost * 0.25);
        double trafficAdjustment = trafficScore < 0.0 ? -1.5 : (trafficScore >= 20.0 ? 1.0 : 0.0);

        double rawPressure = paceComponent + gapComponent + strategyComponent + rivalComponent + trafficAdjustment;
        return Math.max(0.0, Math.min(MAX_TIMING_PRESSURE_SCORE, rawPressure));
    }

    // short-horizon reaction to nearby rival pit events, this shifts urgency
    // toward realistic cover or undercut windows at lap resolution.
    private static double computeRivalPitReactionBoost(LapEvent current, List<LapEvent> laps, int posIndex) {
        if (!TrackStatusCodes.isGreenOrUnknown(current.getTrackStatus())) {
            return 0.0;
        }

        double boost = 0.0;
        if (posIndex > 0) {
            boost += computeNeighborPitBoost(current, laps.get(posIndex - 1), true);
        }
        if (posIndex + 1 < laps.size()) {
            boost += computeNeighborPitBoost(current, laps.get(posIndex + 1), false);
        }
        return Math.min(MAX_RIVAL_PIT_REACTION_BOOST, boost);
    }

    private static double computeNeighborPitBoost(LapEvent current, LapEvent neighbor, boolean ahead) {
        if (neighbor == null || neighbor.getPitInTime() == null) {
            return 0.0;
        }
        int lapDistance = Math.abs(current.getLapNumber() - neighbor.getLapNumber());
        if (lapDistance > RIVAL_PIT_REACTION_LAP_WINDOW) {
            return 0.0;
        }
        return ahead ? RIVAL_PIT_REACTION_BOOST_AHEAD : RIVAL_PIT_REACTION_BOOST_BEHIND;
    }

    // continuous pace score: power 1.5 curve.
    // gentle at low degradation, aggressive at high.
    // ex: 1% deg -> 8.1, 2% deg -> 21.8, 3%+ -> 30.0
    private double computePaceScore(LapEvent current) throws Exception {
        DriverPitState state = driverStates.get(current.getDriver());
        if (state == null || current.getLapTime() == null) {
            return 0.0;
        }
        if (state.getStintBestLap() >= Double.MAX_VALUE) {
            return 0.0;
        }

        // require at least 1 consecutive slow lap to filter one-off blips
        if (state.getConsecutiveSlowLaps() < 1) {
            return 0.0;
        }

        double paceRatio = state.getLastPaceRatio();
        if (paceRatio <= 0) {
            return 0.0;
        }

        // 30 * min(1.0, (paceRatio / 0.03)^1.5)
        double normalized = paceRatio / PACE_CEILING_RATIO;
        return 30.0 * Math.min(1.0, Math.pow(normalized, PACE_CURVE_POWER));
    }

    // +60 if sc or vsc is active (crisp, binary event)
    private static int computeTrackStatusScore(String trackStatus) {
        if (trackStatus == null) {
            return 0;
        }
        return switch (trackStatus) {
            case TrackStatusCodes.SAFETY_CAR, TrackStatusCodes.VIRTUAL_SAFETY_CAR, TrackStatusCodes.VSC_ENDING ->
                TRACK_STATUS_SCORE;
            default ->
                0;
        };
    }

    // continuous traffic score: linear interpolation based on emergence gap.
    // >= 3.0s -> +30 (clean air)
    // 1.0-3.0s -> linear 0 to +30
    // 0.0-1.0s -> linear -30 to 0 (DRS danger zone)
    // < 0.0s -> -30 (stuck behind)
    // bonus +5 if car ahead has old tires (easy pass)
    private TrafficResult computeTrafficResult(
            LapEvent current,
            List<LapEvent> laps,
            int posIndex,
            String currentTrackStatus) {
        TrafficResult result = new TrafficResult();
        result.emergencePosition = current.getPosition();

        Double pitLoss = selectPitLoss(current, currentTrackStatus);
        if (pitLoss == null) {
            return result;
        }

        double cumulativeGap = 0;
        LapEvent physicalCarAhead = null;
        double gapToPhysicalCar = 0;

        for (int j = posIndex + 1; j < laps.size(); j++) {
            LapEvent behind = laps.get(j);
            Double gap = behind.getGapToCarAhead();
            if (gap == null) {
                break;
            }

            cumulativeGap += gap;

            if (cumulativeGap >= pitLoss) {
                if (j == posIndex + 1) {
                    // gap behind > pitLoss, no positions lost
                    result.score = 30.0;
                    return result;
                }
                physicalCarAhead = laps.get(j - 1);
                gapToPhysicalCar = pitLoss - (cumulativeGap - gap);
                break;
            }

            physicalCarAhead = behind;
            gapToPhysicalCar = pitLoss - cumulativeGap;
        }

        if (physicalCarAhead == null) {
            result.score = 30.0;
            return result;
        }

        result.emergencePosition = physicalCarAhead.getPosition() + 1;
        result.gapToPhysicalCar = gapToPhysicalCar;

        // continuous gap-based scoring
        double emergenceGap = gapToPhysicalCar;

        if (emergenceGap >= CLEAN_AIR_GAP) {
            result.score = 30.0;
        } else if (emergenceGap >= DRS_THRESHOLD) {
            // linear interpolation: 1.0s -> 0, 3.0s -> +30
            result.score = 30.0 * (emergenceGap - DRS_THRESHOLD) / (CLEAN_AIR_GAP - DRS_THRESHOLD);
        } else if (emergenceGap >= 0) {
            // DRS danger zone: 0s -> -30, 1.0s -> 0
            result.score = -30.0 * (1.0 - emergenceGap / DRS_THRESHOLD);
        } else {
            result.score = -30.0;
        }

        // bonus for easy pass: car ahead on old tires is significantly slower
        if (physicalCarAhead.getTyreLife() >= EASY_PASS_TYRE_LIFE && result.score < 30.0) {
            result.score += EASY_PASS_BONUS;
            result.score = Math.min(30.0, result.score);
        }

        return result;
    }

    // continuous urgency score: quadratic ramp starting at 70% of max stint.
    // zero until the late stint window, then accelerates.
    // ex: 70% stint -> 0.0, 85% stint -> 7.5, 95% stint -> 20.8, 100%+ -> 30.0
    private double computeUrgencyScore(LapEvent current) throws Exception {
        String compound = current.getCompound();
        int tyreAge = current.getTyreLife();

        Integer maxStint = null;
        if (compound != null) {
            maxStint = maxStintByCompound.get(compound);
        }
        if (maxStint == null) {
            maxStint = defaultMaxStint(compound);
        }

        double tyreRatio = (double) tyreAge / maxStint;

        if (tyreRatio < URGENCY_ONSET_RATIO) {
            return 0.0;
        }

        // 30 * min(1.0, ((tyreRatio - 0.5) / 0.5)^2)
        double normalized = (tyreRatio - URGENCY_ONSET_RATIO) / (1.0 - URGENCY_ONSET_RATIO);
        return 30.0 * Math.min(1.0, normalized * normalized);
    }

    // continuous strategy penalty: how much deficit vs needed stint on next compound.
    // if next compound can cover remaining laps, penalty = 0.
    // otherwise, scales linearly with deficit up to -15.
    private double computeStrategyPenalty(LapEvent current) throws Exception {
        int totalLaps = current.getTotalLaps();
        if (totalLaps <= 0) {
            return 0.0;
        }

        int lapsRemaining = totalLaps - current.getLapNumber();
        if (lapsRemaining <= 0) {
            return 0.0;
        }

        String nextCompound = inferNextCompound(current.getCompound());
        Integer maxStint = maxStintByCompound.get(nextCompound);
        if (maxStint == null) {
            maxStint = defaultMaxStint(nextCompound);
        }

        if (maxStint >= lapsRemaining) {
            return 0.0;
        }

        // deficit ratio: how much of the remaining distance can't be covered
        double deficit = (double) (lapsRemaining - maxStint) / lapsRemaining;
        return -15.0 * Math.min(1.0, deficit * 3.0);
    }

    // end-of-race suppression: logistic sigmoid that smoothly kills pit suggestions
    // in the final laps. the penalty is near-zero until ~85% race completion, then
    // ramps sharply through 90-95%, reaching -100 at 98%+.
    //
    // formula: -100 / (1 + e^(-k * (ratio - midpoint)))
    // with k=15, midpoint=0.92:
    //   85% -> -2.5, 90% -> -18.2, 92% -> -50.0, 95% -> -89.1, 98% -> -99.3
    //
    // this allows SC/VSC (+60) to still suggest pits at 90% (net +42 = MONITOR),
    // but correctly suppresses even SC-driven suggestions at 95%+ (net -29 = killed).
    private static double computeEndOfRacePenalty(LapEvent current) {
        int totalLaps = current.getTotalLaps();
        if (totalLaps <= 0) {
            return 0.0;
        }
        double raceCompletionRatio = (double) current.getLapNumber() / totalLaps;
        return -100.0 / (1.0 + Math.exp(-EOR_SIGMOID_K * (raceCompletionRatio - EOR_SIGMOID_MIDPOINT)));
    }

    // selects pit loss based on track status (green, sc, vsc)
    private static Double selectPitLoss(LapEvent lap, String currentTrackStatus) {
        String status = TrackStatusCodes.normalizeOrGreen(currentTrackStatus);
        return switch (status) {
            case TrackStatusCodes.GREEN ->
                lap.getPitLoss();
            case TrackStatusCodes.VIRTUAL_SAFETY_CAR, TrackStatusCodes.VSC_ENDING ->
                lap.getVscPitLoss();
            case TrackStatusCodes.SAFETY_CAR ->
                lap.getScPitLoss();
            default ->
                null;
        };
    }

    // builds human-readable explanation from active scoring components
    private static String buildSuggestion(double paceScore, int trackStatusScore,
            double trafficScore, double strategyPenalty, double urgencyScore,
            double endOfRacePenalty, double rivalPitReactionBoost,
            double timingPressureScore) {
        List<String> parts = new ArrayList<>();
        if (paceScore > 5.0) {
            parts.add("pace drop");
        }
        if (rivalPitReactionBoost > 0.0) {
            parts.add("rival boxed");
        }
        if (timingPressureScore >= 6.0) {
            parts.add("window tightening");
        } else if (timingPressureScore >= 3.0) {
            parts.add("window forming");
        }
        if (trackStatusScore > 0) {
            parts.add("SC/VSC opportunity");
        }
        if (trafficScore >= 25.0) {
            parts.add("clean air");
        } else if (trafficScore > 0) {
            parts.add("decent gap");
        } else if (trafficScore < -10.0) {
            parts.add("traffic risk");
        }
        if (strategyPenalty < -5.0) {
            parts.add("tight tire window");
        }
        if (urgencyScore >= 20.0) {
            parts.add("tire cliff");
        } else if (urgencyScore >= 10.0) {
            parts.add("closing window");
        }
        if (endOfRacePenalty < -30.0) {
            parts.add("race ending");
        }
        return parts.isEmpty() ? "general" : String.join(" + ", parts);
    }

    private static String inferNextCompound(String current) {
        if (current == null) {
            return "MEDIUM";
        }
        return switch (current.toUpperCase()) {
            case "SOFT" ->
                "MEDIUM";
            case "MEDIUM" ->
                "HARD";
            case "HARD" ->
                "MEDIUM";
            default ->
                "MEDIUM";
        };
    }

    private static int defaultMaxStint(String compound) {
        if (compound == null) {
            return DEFAULT_MEDIUM_STINT;
        }
        return switch (compound.toUpperCase()) {
            case "SOFT" ->
                DEFAULT_SOFT_STINT;
            case "MEDIUM" ->
                DEFAULT_MEDIUM_STINT;
            case "HARD" ->
                DEFAULT_HARD_STINT;
            case "INTERMEDIATE", "WET" ->
                DEFAULT_WET_STINT;
            default ->
                DEFAULT_MEDIUM_STINT;
        };
    }

    private static class TrafficResult {

        double score = 0;
        int emergencePosition = 0;
        double gapToPhysicalCar = 0;
    }
}
