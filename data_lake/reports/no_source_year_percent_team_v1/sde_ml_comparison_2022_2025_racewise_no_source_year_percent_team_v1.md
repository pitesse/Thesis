# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-05-01T17:09:44.823836+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Significance summary: data_lake/reports/no_source_year_percent_team_v1/significance_summary_2022_2025_racewise_no_source_year_percent_team_v1.csv
- Significance tests: data_lake/reports/no_source_year_percent_team_v1/significance_tests_2022_2025_racewise_no_source_year_percent_team_v1.csv
- SDE comparator: data_lake/reports/no_source_year_percent_team_v1/heuristic_comparator_2022_2025_racewise_no_source_year_percent_team_v1.csv
- ML comparator: data_lake/reports/no_source_year_percent_team_v1/ml_comparator_2022_2025_racewise_no_source_year_percent_team_v1.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 6323 | 1020 | 5303 | 749 | 271 | 0.734314 | [0.706365, 0.760504] |
| ML | 1814 | 248 | 1566 | 210 | 38 | 0.846774 | [0.796684, 0.886285] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | 3.699959 | 0.000215634 | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 0.000000 | 1 | paired only on shared race driver lap keys, overlap_n=44, discordant=0 |
| mcnemar_exact | overlap_scored_keys_only | 0.000000 | 1 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=0, sde_failure_ml_success=0 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): 0.112460
- Scored-row delta (ML - SDE): -772
- Actionable-row delta (ML - SDE): -4509
- Scored ratio (ML / SDE): 0.243137
- Actionable ratio (ML / SDE): 0.286889
- Overlap scored keys: 44
- Overlap ratio vs SDE scored: 0.043137, vs ML scored: 0.177419

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 155 | 0.645161 | 90 | 0.666667 | 0.021505 | -65 |
| 2023 | 226 | 0.721239 | 31 | 0.903226 | 0.181987 | -195 |
| 2024 | 299 | 0.759197 | 56 | 0.946429 | 0.187231 | -243 |
| 2025 | 340 | 0.761765 | 71 | 0.971831 | 0.210066 | -269 |

## Top Exclusion Reasons
| Model | Exclusion Reason | Count | Share Within Model Exclusions |
| --- | --- | ---: | ---: |
| SDE | NO_MATCH_WITHIN_HORIZON | 4873 | 0.918914 |
| SDE | UNRESOLVED_MISSING_POST_GAP | 240 | 0.045257 |
| SDE | UNRESOLVED_INCIDENT_FILTER | 148 | 0.027909 |
| SDE | WEATHER_SURVIVAL_STOP | 42 | 0.007920 |
| ML | NO_MATCH_WITHIN_HORIZON | 1512 | 0.965517 |
| ML | UNRESOLVED_MISSING_POST_GAP | 29 | 0.018519 |
| ML | UNRESOLVED_INCIDENT_FILTER | 21 | 0.013410 |
| ML | UNRESOLVED_MISSING_PRE_GAP | 2 | 0.001277 |
| ML | WEATHER_SURVIVAL_STOP | 2 | 0.001277 |

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
- Summary CSV: data_lake/reports/no_source_year_percent_team_v1/sde_ml_comparison_summary_2022_2025_racewise_no_source_year_percent_team_v1.csv
- By-year CSV: data_lake/reports/no_source_year_percent_team_v1/sde_ml_comparison_by_year_2022_2025_racewise_no_source_year_percent_team_v1.csv
