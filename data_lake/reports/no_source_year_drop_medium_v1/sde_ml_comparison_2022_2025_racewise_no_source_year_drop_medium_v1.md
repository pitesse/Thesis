# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-05-05T18:44:13.356298+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Significance summary: data_lake/reports/no_source_year_drop_medium_v1/significance_summary_2022_2025_racewise_no_source_year_drop_medium_v1.csv
- Significance tests: data_lake/reports/no_source_year_drop_medium_v1/significance_tests_2022_2025_racewise_no_source_year_drop_medium_v1.csv
- SDE comparator: data_lake/reports/no_source_year_drop_medium_v1/heuristic_comparator_2022_2025_racewise_no_source_year_drop_medium_v1.csv
- ML comparator: data_lake/reports/no_source_year_drop_medium_v1/ml_comparator_2022_2025_racewise_no_source_year_drop_medium_v1.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 5453 | 487 | 4966 | 306 | 181 | 0.628337 | [0.584569, 0.670095] |
| ML | 561 | 87 | 474 | 48 | 39 | 0.551724 | [0.447245, 0.651828] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | -1.353840 | 0.175787 | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 0.000000 | 1 | paired only on shared race driver lap keys, overlap_n=24, discordant=0 |
| mcnemar_exact | overlap_scored_keys_only | 0.000000 | 1 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=0, sde_failure_ml_success=0 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): -0.076613
- Scored-row delta (ML - SDE): -400
- Actionable-row delta (ML - SDE): -4892
- Scored ratio (ML / SDE): 0.178645
- Actionable ratio (ML / SDE): 0.102879
- Overlap scored keys: 24
- Overlap ratio vs SDE scored: 0.049281, vs ML scored: 0.275862

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 136 | 0.632353 | 49 | 0.571429 | -0.060924 | -87 |
| 2023 | 89 | 0.662921 | 14 | 0.714286 | 0.051364 | -75 |
| 2024 | 136 | 0.632353 | 21 | 0.333333 | -0.299020 | -115 |
| 2025 | 126 | 0.595238 | 3 | 1.000000 | 0.404762 | -123 |

## Top Exclusion Reasons
| Model | Exclusion Reason | Count | Share Within Model Exclusions |
| --- | --- | ---: | ---: |
| SDE | NO_MATCH_WITHIN_HORIZON | 4619 | 0.930125 |
| SDE | UNRESOLVED_MISSING_POST_GAP | 186 | 0.037455 |
| SDE | UNRESOLVED_INCIDENT_FILTER | 126 | 0.025373 |
| SDE | WEATHER_SURVIVAL_STOP | 35 | 0.007048 |
| ML | NO_MATCH_WITHIN_HORIZON | 452 | 0.953586 |
| ML | UNRESOLVED_MISSING_POST_GAP | 15 | 0.031646 |
| ML | UNRESOLVED_INCIDENT_FILTER | 6 | 0.012658 |
| ML | WEATHER_SURVIVAL_STOP | 1 | 0.002110 |

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
- Summary CSV: data_lake/reports/no_source_year_drop_medium_v1/sde_ml_comparison_summary_2022_2025_racewise_no_source_year_drop_medium_v1.csv
- By-year CSV: data_lake/reports/no_source_year_drop_medium_v1/sde_ml_comparison_by_year_2022_2025_racewise_no_source_year_drop_medium_v1.csv
