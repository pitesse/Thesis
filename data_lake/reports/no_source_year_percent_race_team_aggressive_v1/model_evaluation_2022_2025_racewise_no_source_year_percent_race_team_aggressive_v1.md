# Unified Model Evaluation Report (2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1)

## Scope
- Years: [2022, 2023, 2024, 2025]
- Horizon: H=2
- Comparator source token: year=9999, season_tag=merged
- Feature profile: percent_race_team_aggressive_v1
- Excluded features: team,rainfall,hasGapAhead,hasGapBehind,lapTime,pitLoss,pace_trend,has_drop_zone_data
- Track-agnostic mode: track_percentage_race_team_v1

## Why These Tests
- Leakage-safe grouped validation by race (Roberts et al., 2017).
- Horizon-based comparator matching and temporal validity checks (Brookshire, 2024).
- Imbalance-aware precision-focused policy evaluation (Elkan, 2001; Saito and Rehmsmeier, 2015).
- Calibration reliability and operational readiness gates before deployment claims.

## Results
| Test ID | Test | Why | Status | Metric | Value | Threshold | Artifact |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B1 | ML precision delta vs SDE | Checks if ML improves precision under the same comparator semantics. | PASS | precision_delta | 0.176539 | 0.000000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/significance_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| B2 | Two-proportion z significance | Tests whether observed precision difference is statistically significant. | PASS | p_value | 0.000000 | 0.050000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/significance_tests_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| C1 | Reference precision floor | Verifies threshold policy remains at or above the SDE precision floor. | PASS | reference_precision | 0.862927 | 0.734314 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/threshold_frontier_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| C2 | Lookahead no-match dominance | Checks that most exclusions are horizon-related, supporting comparator interpretation. | PASS | no_match_rate | 0.964510 | 0.900000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/threshold_frontier_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| D1 | Constrained precision | Ensures calibrated constrained policy is precise enough for strategy actions. | FAIL | constrained_precision | 0.176503 | 0.600000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/calibration_policy_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| D2 | Precision-floor reachability | Measures whether candidate thresholds can reliably satisfy precision constraints. | FAIL | reachability_ratio | 0.732908 | 0.900000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/calibration_policy_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| D3 | Fallback rate | Checks constrained policy stability when precision floor is hard to satisfy. | PASS | fallback_rate | 0.010526 | 0.100000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/calibration_policy_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| F1 | Training-serving parity gate | Guards against feature/schema skew between offline training and live serving. | PASS | feature_parity_overall_gate | 1.000000 | 1.000000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/feature_parity_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| G1 | Latency gate | Checks p95 end-to-end inference latency against operational budget. | PASS | latency_p95_total_ms | 4.816250 | 500.000000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/live_latency_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| G2 | Availability gate | Ensures prediction path remains available across replayed events. | PASS | availability_pct | 100.000000 | 99.000000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/live_latency_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| H1 | Integrated deployment decision | Combines B/C/D/F/G evidence into one actionable readiness decision. | NO_GO | integrated_gate_decision | N/A | N/A | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/integrated_gate_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| J1 | Split-integrity gate | Verifies grouped race CV and OOF coverage assumptions are preserved. | FAIL | split_integrity_overall | 0.000000 | 1.000000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/split_integrity_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |
| J2 | Comparator-invariance gate | Ensures fairness contract (actionable-only, fixed horizon, one-to-one matching) stays frozen. | PASS | comparator_invariance_overall | 1.000000 | 1.000000 | data_lake/reports/no_source_year_percent_race_team_aggressive_v1/comparator_invariance_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv |

## Integrated Decision
- Decision: NO_GO
- Note: confidence=LOW; core validity gate failed in D

## Core Artifacts
- Unified summary CSV: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/model_evaluation_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv
- Integrated gate report: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/integrated_gate_report_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.txt
- Dedicated SDE vs ML report: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/sde_ml_comparison_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.md
- Dedicated SDE vs ML summary: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/sde_ml_comparison_summary_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv
- Dedicated SDE vs ML by year: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/sde_ml_comparison_by_year_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv
- Comparator files: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/heuristic_comparator_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv, data_lake/reports/no_source_year_percent_race_team_aggressive_v1/ml_comparator_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.csv
- Threshold sweep report: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/threshold_frontier_report_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.txt
- Calibration report: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/calibration_policy_report_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.txt
- Parity report: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/feature_parity_report_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.txt
- Latency report: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/live_latency_report_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.txt
- Split-integrity report: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/split_integrity_report_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.txt
- Comparator-invariance report: data_lake/reports/no_source_year_percent_race_team_aggressive_v1/comparator_invariance_report_2022_2025_racewise_no_source_year_percent_race_team_aggressive_v1.txt
