# Unified Model Evaluation Report (2022_2025_racewise_no_source_year_track_agnostic_v1_strict)

## Scope
- Years: [2022, 2023, 2024, 2025]
- Horizon: H=2
- Comparator source token: year=9999, season_tag=merged
- Feature profile: track_agnostic_v1
- Excluded features: team,rainfall,hasGapAhead,hasGapBehind,trackTemp,airTemp,humidity,speedTrap,lapTime
- Track-agnostic mode: track_agnostic_v1

## Why These Tests
- Leakage-safe grouped validation by race (Roberts et al., 2017).
- Horizon-based comparator matching and temporal validity checks (Brookshire, 2024).
- Imbalance-aware precision-focused policy evaluation (Elkan, 2001; Saito and Rehmsmeier, 2015).
- Calibration reliability and operational readiness gates before deployment claims.

## Results
| Test ID | Test | Why | Status | Metric | Value | Threshold | Artifact |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B1 | ML precision delta vs SDE | Checks if ML improves precision under the same comparator semantics. | PASS | precision_delta | 0.110281 | 0.000000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/significance_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| B2 | Two-proportion z significance | Tests whether observed precision difference is statistically significant. | PASS | p_value | 0.003867 | 0.050000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/significance_tests_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| C1 | Reference precision floor | Verifies threshold policy remains at or above the SDE precision floor. | PASS | reference_precision | 0.853743 | 0.734314 | data_lake/reports/no_source_year_track_agnostic_v1_strict/threshold_frontier_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| C2 | Lookahead no-match dominance | Checks that most exclusions are horizon-related, supporting comparator interpretation. | PASS | no_match_rate | 0.965991 | 0.900000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/threshold_frontier_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| D1 | Constrained precision | Ensures calibrated constrained policy is precise enough for strategy actions. | FAIL | constrained_precision | 0.136247 | 0.600000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/calibration_policy_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| D2 | Precision-floor reachability | Measures whether candidate thresholds can reliably satisfy precision constraints. | FAIL | reachability_ratio | 0.652844 | 0.900000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/calibration_policy_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| D3 | Fallback rate | Checks constrained policy stability when precision floor is hard to satisfy. | PASS | fallback_rate | 0.073684 | 0.100000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/calibration_policy_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| F1 | Training-serving parity gate | Guards against feature/schema skew between offline training and live serving. | FAIL | feature_parity_overall_gate | 0.000000 | 1.000000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/feature_parity_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| G1 | Latency gate | Checks p95 end-to-end inference latency against operational budget. | PASS | latency_p95_total_ms | 7.263786 | 500.000000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/live_latency_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| G2 | Availability gate | Ensures prediction path remains available across replayed events. | PASS | availability_pct | 100.000000 | 99.000000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/live_latency_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| H1 | Integrated deployment decision | Combines B/C/D/F/G evidence into one actionable readiness decision. | NO_GO | integrated_gate_decision | N/A | N/A | data_lake/reports/no_source_year_track_agnostic_v1_strict/integrated_gate_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| J1 | Split-integrity gate | Verifies grouped race CV and OOF coverage assumptions are preserved. | FAIL | split_integrity_overall | 0.000000 | 1.000000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/split_integrity_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |
| J2 | Comparator-invariance gate | Ensures fairness contract (actionable-only, fixed horizon, one-to-one matching) stays frozen. | PASS | comparator_invariance_overall | 1.000000 | 1.000000 | data_lake/reports/no_source_year_track_agnostic_v1_strict/comparator_invariance_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv |

## Integrated Decision
- Decision: NO_GO
- Note: confidence=LOW; core validity gate failed in D/F

## Core Artifacts
- Unified summary CSV: data_lake/reports/no_source_year_track_agnostic_v1_strict/model_evaluation_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv
- Integrated gate report: data_lake/reports/no_source_year_track_agnostic_v1_strict/integrated_gate_report_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.txt
- Dedicated SDE vs ML report: data_lake/reports/no_source_year_track_agnostic_v1_strict/sde_ml_comparison_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.md
- Dedicated SDE vs ML summary: data_lake/reports/no_source_year_track_agnostic_v1_strict/sde_ml_comparison_summary_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv
- Dedicated SDE vs ML by year: data_lake/reports/no_source_year_track_agnostic_v1_strict/sde_ml_comparison_by_year_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv
- Comparator files: data_lake/reports/no_source_year_track_agnostic_v1_strict/heuristic_comparator_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv, data_lake/reports/no_source_year_track_agnostic_v1_strict/ml_comparator_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.csv
- Threshold sweep report: data_lake/reports/no_source_year_track_agnostic_v1_strict/threshold_frontier_report_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.txt
- Calibration report: data_lake/reports/no_source_year_track_agnostic_v1_strict/calibration_policy_report_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.txt
- Parity report: data_lake/reports/no_source_year_track_agnostic_v1_strict/feature_parity_report_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.txt
- Latency report: data_lake/reports/no_source_year_track_agnostic_v1_strict/live_latency_report_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.txt
- Split-integrity report: data_lake/reports/no_source_year_track_agnostic_v1_strict/split_integrity_report_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.txt
- Comparator-invariance report: data_lake/reports/no_source_year_track_agnostic_v1_strict/comparator_invariance_report_2022_2025_racewise_no_source_year_track_agnostic_v1_strict.txt
