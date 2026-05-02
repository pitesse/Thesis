# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-05-01T14:48:04.484838+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Significance summary: data_lake/reports/no_source_year_drop_medium_track_agnostic_v1/significance_summary_2022_2025_racewise_no_source_year_drop_medium_track_agnostic_v1.csv
- Significance tests: data_lake/reports/no_source_year_drop_medium_track_agnostic_v1/significance_tests_2022_2025_racewise_no_source_year_drop_medium_track_agnostic_v1.csv
- SDE comparator: data_lake/reports/no_source_year_drop_medium_track_agnostic_v1/heuristic_comparator_2022_2025_racewise_no_source_year_drop_medium_track_agnostic_v1.csv
- ML comparator: data_lake/reports/no_source_year_drop_medium_track_agnostic_v1/ml_comparator_2022_2025_racewise_no_source_year_drop_medium_track_agnostic_v1.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 6323 | 1020 | 5303 | 749 | 271 | 0.734314 | [0.706365, 0.760504] |
| ML | 2723 | 279 | 2444 | 245 | 34 | 0.878136 | [0.834532, 0.911469] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | 5.022158 | 5.1094e-07 | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 0.000000 | 1 | paired only on shared race driver lap keys, overlap_n=47, discordant=0 |
| mcnemar_exact | overlap_scored_keys_only | 0.000000 | 1 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=0, sde_failure_ml_success=0 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): 0.143822
- Scored-row delta (ML - SDE): -741
- Actionable-row delta (ML - SDE): -3600
- Scored ratio (ML / SDE): 0.273529
- Actionable ratio (ML / SDE): 0.430650
- Overlap scored keys: 47
- Overlap ratio vs SDE scored: 0.046078, vs ML scored: 0.168459

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 155 | 0.645161 | 86 | 0.686047 | 0.040885 | -69 |
| 2023 | 226 | 0.721239 | 37 | 0.945946 | 0.224707 | -189 |
| 2024 | 299 | 0.759197 | 75 | 0.973333 | 0.214136 | -224 |
| 2025 | 340 | 0.761765 | 81 | 0.962963 | 0.201198 | -259 |

## Top Exclusion Reasons
| Model | Exclusion Reason | Count | Share Within Model Exclusions |
| --- | --- | ---: | ---: |
| SDE | NO_MATCH_WITHIN_HORIZON | 4873 | 0.918914 |
| SDE | UNRESOLVED_MISSING_POST_GAP | 240 | 0.045257 |
| SDE | UNRESOLVED_INCIDENT_FILTER | 148 | 0.027909 |
| SDE | WEATHER_SURVIVAL_STOP | 42 | 0.007920 |
| ML | NO_MATCH_WITHIN_HORIZON | 2386 | 0.976268 |
| ML | UNRESOLVED_MISSING_POST_GAP | 28 | 0.011457 |
| ML | UNRESOLVED_INCIDENT_FILTER | 25 | 0.010229 |
| ML | WEATHER_SURVIVAL_STOP | 3 | 0.001227 |
| ML | UNRESOLVED_MISSING_PRE_GAP | 2 | 0.000818 |

## Interpretation and Limits
- Primary inferential claim should be based on two-proportion z under independent scored-row assumption.
- McNemar results are overlap-only sensitivity checks and should not replace the primary inference when overlap is limited.
- Coverage deltas should be discussed jointly with precision deltas to avoid selective reporting.
- Per-year rows with small scored support should be treated as directional, not as stand-alone inferential evidence.

## Paper Grounding
- Fair comparator and leakage-safe evaluation: Roberts 2017, Brookshire 2024.
- Comparative test rigor and uncertainty: Dietterich 1998, Walters 2022.
- Imbalance-aware precision focus: Saito and Rehmsmeier 2015, Davis and Goadrich 2006.

## Generated Artifacts
- Summary CSV: data_lake/reports/no_source_year_drop_medium_track_agnostic_v1/sde_ml_comparison_summary_2022_2025_racewise_no_source_year_drop_medium_track_agnostic_v1.csv
- By-year CSV: data_lake/reports/no_source_year_drop_medium_track_agnostic_v1/sde_ml_comparison_by_year_2022_2025_racewise_no_source_year_drop_medium_track_agnostic_v1.csv
