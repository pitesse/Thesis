# Unified Model Evaluation Report (2022_2025_no_source_year_drop_aggressive_v1_candidate)

## Scope
- Years: [2022, 2023, 2024, 2025]
- Horizon: H=2
- Comparator source token: year=9999, season_tag=merged
- Feature profile: drop_aggressive_v1_candidate
- Excluded features: team,rainfall,hasGapAhead,hasGapBehind,pitLoss,pace_trend,has_drop_zone_data
- Track-agnostic mode: off

## Why These Tests
- Leakage-safe grouped validation by race (Roberts et al., 2017).
- Horizon-based comparator matching and temporal validity checks (Brookshire, 2024).
- Imbalance-aware precision-focused policy evaluation (Elkan, 2001; Saito and Rehmsmeier, 2015).
- Calibration reliability and operational readiness gates before deployment claims.

## Results
| Test ID | Test | Why | Status | Metric | Value | Threshold | Artifact |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B1 | ML precision delta vs SDE | Checks if ML improves precision under the same comparator semantics. | PASS | precision_delta | 0.224416 | 0.000000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/significance_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| B2 | Two-proportion z significance | Tests whether observed precision difference is statistically significant. | PASS | p_value | 0.000000 | 0.050000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/significance_tests_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| C1 | Reference precision floor | Verifies threshold policy remains at or above the SDE precision floor. | PASS | reference_precision | 0.887122 | 0.734314 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/threshold_frontier_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| C2 | Lookahead no-match dominance | Checks that most exclusions are horizon-related, supporting comparator interpretation. | PASS | no_match_rate | 0.968154 | 0.900000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/threshold_frontier_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| D1 | Constrained precision | Ensures calibrated constrained policy is precise enough for strategy actions. | PASS | constrained_precision | 0.828283 | 0.600000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/calibration_policy_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| D2 | Precision-floor reachability | Measures whether candidate thresholds can reliably satisfy precision constraints. | PASS | reachability_ratio | 0.903221 | 0.900000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/calibration_policy_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| D3 | Fallback rate | Checks constrained policy stability when precision floor is hard to satisfy. | PASS | fallback_rate | 0.013514 | 0.100000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/calibration_policy_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| F1 | Training-serving parity gate | Guards against feature/schema skew between offline training and live serving. | PASS | feature_parity_overall_gate | 1.000000 | 1.000000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/feature_parity_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| G1 | Latency gate | Checks p95 end-to-end inference latency against operational budget. | PASS | latency_p95_total_ms | 7.520073 | 500.000000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/live_latency_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| G2 | Availability gate | Ensures prediction path remains available across replayed events. | PASS | availability_pct | 100.000000 | 99.000000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/live_latency_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| H1 | Integrated deployment decision | Combines B/C/D/F/G evidence into one actionable readiness decision. | GO | integrated_gate_decision | N/A | N/A | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/integrated_gate_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| J1 | Split-integrity gate | Verifies grouped race CV and OOF coverage assumptions are preserved. | FAIL | split_integrity_overall | 0.000000 | 1.000000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/split_integrity_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |
| J2 | Comparator-invariance gate | Ensures fairness contract (actionable-only, fixed horizon, one-to-one matching) stays frozen. | PASS | comparator_invariance_overall | 1.000000 | 1.000000 | data_lake/reports/no_source_year_drop_aggressive_v1_candidate/comparator_invariance_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv |

## Integrated Decision
- Decision: GO
- Note: confidence=HIGH; all integrated gates passed

## Core Artifacts
- Unified summary CSV: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/model_evaluation_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
- Integrated gate report: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/integrated_gate_report_2022_2025_no_source_year_drop_aggressive_v1_candidate.txt
- Dedicated SDE vs ML report: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/sde_ml_comparison_2022_2025_no_source_year_drop_aggressive_v1_candidate.md
- Dedicated SDE vs ML summary: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/sde_ml_comparison_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
- Dedicated SDE vs ML by year: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/sde_ml_comparison_by_year_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
- Comparator files: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/heuristic_comparator_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv, data_lake/reports/no_source_year_drop_aggressive_v1_candidate/ml_comparator_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
- Threshold sweep report: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/threshold_frontier_report_2022_2025_no_source_year_drop_aggressive_v1_candidate.txt
- Calibration report: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/calibration_policy_report_2022_2025_no_source_year_drop_aggressive_v1_candidate.txt
- Parity report: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/feature_parity_report_2022_2025_no_source_year_drop_aggressive_v1_candidate.txt
- Latency report: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/live_latency_report_2022_2025_no_source_year_drop_aggressive_v1_candidate.txt
- Split-integrity report: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/split_integrity_report_2022_2025_no_source_year_drop_aggressive_v1_candidate.txt
- Comparator-invariance report: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/comparator_invariance_report_2022_2025_no_source_year_drop_aggressive_v1_candidate.txt
