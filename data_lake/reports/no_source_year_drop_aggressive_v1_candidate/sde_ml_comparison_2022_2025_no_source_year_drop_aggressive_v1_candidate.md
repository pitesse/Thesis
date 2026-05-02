# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-05-01T15:12:20.686971+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Significance summary: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/significance_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
- Significance tests: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/significance_tests_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
- SDE comparator: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/heuristic_comparator_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
- ML comparator: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/ml_comparator_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 6323 | 1020 | 5303 | 749 | 271 | 0.734314 | [0.706365, 0.760504] |
| ML | 495 | 315 | 180 | 302 | 13 | 0.958730 | [0.930681, 0.975726] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | 8.507268 | <1e-16 (underflow) | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 0.000000 | 1 | paired only on shared race driver lap keys, overlap_n=33, discordant=0 |
| mcnemar_exact | overlap_scored_keys_only | 0.000000 | 1 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=0, sde_failure_ml_success=0 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): 0.224416
- Scored-row delta (ML - SDE): -705
- Actionable-row delta (ML - SDE): -5828
- Scored ratio (ML / SDE): 0.308824
- Actionable ratio (ML / SDE): 0.078286
- Overlap scored keys: 33
- Overlap ratio vs SDE scored: 0.032353, vs ML scored: 0.104762

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 155 | 0.645161 | N/A | N/A | N/A | N/A |
| 2023 | 226 | 0.721239 | 51 | 0.960784 | 0.239545 | -175 |
| 2024 | 299 | 0.759197 | 110 | 0.945455 | 0.186257 | -189 |
| 2025 | 340 | 0.761765 | 154 | 0.967532 | 0.205768 | -186 |

## Top Exclusion Reasons
| Model | Exclusion Reason | Count | Share Within Model Exclusions |
| --- | --- | ---: | ---: |
| SDE | NO_MATCH_WITHIN_HORIZON | 4873 | 0.918914 |
| SDE | UNRESOLVED_MISSING_POST_GAP | 240 | 0.045257 |
| SDE | UNRESOLVED_INCIDENT_FILTER | 148 | 0.027909 |
| SDE | WEATHER_SURVIVAL_STOP | 42 | 0.007920 |
| ML | NO_MATCH_WITHIN_HORIZON | 169 | 0.938889 |
| ML | UNRESOLVED_MISSING_POST_GAP | 8 | 0.044444 |
| ML | WEATHER_SURVIVAL_STOP | 2 | 0.011111 |
| ML | UNRESOLVED_INCIDENT_FILTER | 1 | 0.005556 |

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
- Summary CSV: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/sde_ml_comparison_summary_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
- By-year CSV: data_lake/reports/no_source_year_drop_aggressive_v1_candidate/sde_ml_comparison_by_year_2022_2025_no_source_year_drop_aggressive_v1_candidate.csv
