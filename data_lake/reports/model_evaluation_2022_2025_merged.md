# Unified Model Evaluation Report (2022_2025_merged)

## Scope
- Years: [2022, 2023, 2024, 2025]
- Horizon: H=2
- Comparator source token: year=9999, season_tag=merged

## Why These Tests
- Leakage-safe grouped validation by race (Roberts et al., 2017).
- Horizon-based comparator matching and temporal validity checks (Brookshire, 2024).
- Imbalance-aware precision-focused policy evaluation (Elkan, 2001; Saito and Rehmsmeier, 2015).
- Calibration reliability and operational readiness gates before deployment claims.

## Results
| Test ID | Test | Why | Status | Metric | Value | Threshold | Artifact |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B1 | ML precision delta vs SDE | Checks if ML improves precision under the same comparator semantics. | PASS | precision_delta | 0.225456 | 0.000000 | data_lake/reports/phase_b_significance_summary_2022_2025_merged.csv |
| B2 | Two-proportion z significance | Tests whether observed precision difference is statistically significant. | PASS | p_value | 0.000000 | 0.050000 | data_lake/reports/phase_b_significance_tests_2022_2025_merged.csv |
| C1 | Reference precision floor | Verifies threshold policy remains at or above the SDE precision floor. | PASS | reference_precision | 0.868714 | 0.734314 | data_lake/reports/phase_c_threshold_sweep_2022_2025_merged.csv |
| C2 | Lookahead no-match dominance | Checks that most exclusions are horizon-related, supporting comparator interpretation. | PASS | no_match_rate | 0.971099 | 0.900000 | data_lake/reports/phase_c_threshold_sweep_2022_2025_merged.csv |
| D1 | Constrained precision | Ensures calibrated constrained policy is precise enough for strategy actions. | PASS | constrained_precision | 0.807556 | 0.600000 | data_lake/reports/phase_d_calibration_policy_summary_2022_2025_merged.csv |
| D2 | Precision-floor reachability | Measures whether candidate thresholds can reliably satisfy precision constraints. | PASS | reachability_ratio | 0.981818 | 0.900000 | data_lake/reports/phase_d_calibration_policy_summary_2022_2025_merged.csv |
| D3 | Fallback rate | Checks constrained policy stability when precision floor is hard to satisfy. | PASS | fallback_rate | 0.000000 | 0.100000 | data_lake/reports/phase_d_calibration_policy_summary_2022_2025_merged.csv |
| F1 | Training-serving parity gate | Guards against feature/schema skew between offline training and live serving. | PASS | phase_f_overall_gate | 1.000000 | 1.000000 | data_lake/reports/phase_f_parity_summary_2022_2025_merged.csv |
| G1 | Latency gate | Checks p95 end-to-end inference latency against operational budget. | PASS | latency_p95_total_ms | 5.615486 | 500.000000 | data_lake/reports/phase_g_latency_summary_2022_2025_merged.csv |
| G2 | Availability gate | Ensures prediction path remains available across replayed events. | PASS | availability_pct | 100.000000 | 99.000000 | data_lake/reports/phase_g_latency_summary_2022_2025_merged.csv |
| H1 | Integrated deployment decision | Combines B/C/D/F/G evidence into one actionable readiness decision. | GO | phase_h_decision | N/A | N/A | data_lake/reports/phase_h_unified_gate_2022_2025_merged.csv |
| J1 | Split-integrity gate | Verifies grouped race CV and OOF coverage assumptions are preserved. | PASS | phase_j_split_integrity_overall | 1.000000 | 1.000000 | data_lake/reports/phase_j_split_integrity_summary_2022_2025_merged.csv |
| J2 | Comparator-invariance gate | Ensures fairness contract (actionable-only, fixed horizon, one-to-one matching) stays frozen. | PASS | phase_j_comparator_invariance_overall | 1.000000 | 1.000000 | data_lake/reports/phase_j_comparator_invariance_summary_2022_2025_merged.csv |

## Integrated Decision
- Decision: GO
- Note: confidence=HIGH; all integrated gates passed

## Core Artifacts
- Unified summary CSV: data_lake/reports/model_evaluation_2022_2025_merged.csv
- Phase H report: data_lake/reports/phase_h_unified_gate_report_2022_2025_merged.txt
- Phase B dedicated comparison report: data_lake/reports/phase_b_sde_ml_comparison_2022_2025_merged.md
- Phase B dedicated comparison summary: data_lake/reports/phase_b_sde_ml_comparison_summary_2022_2025_merged.csv
- Phase B dedicated comparison by year: data_lake/reports/phase_b_sde_ml_comparison_by_year_2022_2025_merged.csv
- Comparator files: data_lake/reports/heuristic_comparator_2022_2025_merged.csv, data_lake/reports/ml_comparator_2022_2025_merged.csv
- Threshold sweep report: data_lake/reports/phase_c_threshold_sweep_report_2022_2025_merged.txt
- Calibration report: data_lake/reports/phase_d_calibration_policy_report_2022_2025_merged.txt
- Parity report: data_lake/reports/phase_f_parity_report_2022_2025_merged.txt
- Latency report: data_lake/reports/phase_g_latency_report_2022_2025_merged.txt
- Split-integrity report: data_lake/reports/phase_j_split_integrity_report_2022_2025_merged.txt
- Comparator-invariance report: data_lake/reports/phase_j_comparator_invariance_report_2022_2025_merged.txt
