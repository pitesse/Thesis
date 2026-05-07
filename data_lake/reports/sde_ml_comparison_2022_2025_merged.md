# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-05-06T13:55:27.644949+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Significance summary: data_lake/reports/significance_summary_2022_2025_merged.csv
- Significance tests: data_lake/reports/significance_tests_2022_2025_merged.csv
- SDE comparator: data_lake/reports/heuristic_comparator_2022_2025_merged.csv
- ML comparator: data_lake/reports/ml_comparator_2022_2025_merged.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 5453 | 486 | 4967 | 305 | 181 | 0.627572 | [0.583747, 0.669396] |
| ML | 210 | 51 | 159 | 36 | 15 | 0.705882 | [0.570009, 0.812913] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | 1.105107 | 0.269113 | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 0.000000 | 1 | paired only on shared race driver lap keys, overlap_n=11, discordant=0 |
| mcnemar_exact | overlap_scored_keys_only | 0.000000 | 1 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=0, sde_failure_ml_success=0 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): 0.078310
- Scored-row delta (ML - SDE): -435
- Actionable-row delta (ML - SDE): -5243
- Scored ratio (ML / SDE): 0.104938
- Actionable ratio (ML / SDE): 0.038511
- Overlap scored keys: 11
- Overlap ratio vs SDE scored: 0.022634, vs ML scored: 0.215686

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 133 | 0.624060 | N/A | N/A | N/A | N/A |
| 2023 | 91 | 0.670330 | 31 | 0.806452 | 0.136122 | -60 |
| 2024 | 134 | 0.634328 | 17 | 0.529412 | -0.104917 | -117 |
| 2025 | 128 | 0.593750 | 3 | 0.666667 | 0.072917 | -125 |

## Top Exclusion Reasons
| Model | Exclusion Reason | Count | Share Within Model Exclusions |
| --- | --- | ---: | ---: |
| SDE | NO_MATCH_WITHIN_HORIZON | 4619 | 0.929938 |
| SDE | UNRESOLVED_MISSING_POST_GAP | 186 | 0.037447 |
| SDE | UNRESOLVED_INCIDENT_FILTER | 127 | 0.025569 |
| SDE | WEATHER_SURVIVAL_STOP | 35 | 0.007047 |
| ML | NO_MATCH_WITHIN_HORIZON | 152 | 0.955975 |
| ML | UNRESOLVED_MISSING_POST_GAP | 4 | 0.025157 |
| ML | UNRESOLVED_INCIDENT_FILTER | 3 | 0.018868 |

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
- Summary CSV: data_lake/reports/sde_ml_comparison_summary_2022_2025_merged.csv
- By-year CSV: data_lake/reports/sde_ml_comparison_by_year_2022_2025_merged.csv
