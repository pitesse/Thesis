# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-05-01T16:56:35.357885+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Significance summary: data_lake/reports/no_source_year_percent_conservative_v1/significance_summary_2022_2025_racewise_no_source_year_percent_conservative_v1.csv
- Significance tests: data_lake/reports/no_source_year_percent_conservative_v1/significance_tests_2022_2025_racewise_no_source_year_percent_conservative_v1.csv
- SDE comparator: data_lake/reports/no_source_year_percent_conservative_v1/heuristic_comparator_2022_2025_racewise_no_source_year_percent_conservative_v1.csv
- ML comparator: data_lake/reports/no_source_year_percent_conservative_v1/ml_comparator_2022_2025_racewise_no_source_year_percent_conservative_v1.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 6323 | 1020 | 5303 | 749 | 271 | 0.734314 | [0.706365, 0.760504] |
| ML | 1893 | 244 | 1649 | 207 | 37 | 0.848361 | [0.797982, 0.887940] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | 3.727759 | 0.00019319 | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 0.000000 | 1 | paired only on shared race driver lap keys, overlap_n=37, discordant=0 |
| mcnemar_exact | overlap_scored_keys_only | 0.000000 | 1 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=0, sde_failure_ml_success=0 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): 0.114047
- Scored-row delta (ML - SDE): -776
- Actionable-row delta (ML - SDE): -4430
- Scored ratio (ML / SDE): 0.239216
- Actionable ratio (ML / SDE): 0.299383
- Overlap scored keys: 37
- Overlap ratio vs SDE scored: 0.036275, vs ML scored: 0.151639

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 155 | 0.645161 | 89 | 0.651685 | 0.006524 | -66 |
| 2023 | 226 | 0.721239 | 28 | 0.928571 | 0.207332 | -198 |
| 2024 | 299 | 0.759197 | 65 | 0.953846 | 0.194649 | -234 |
| 2025 | 340 | 0.761765 | 62 | 0.983871 | 0.222106 | -278 |

## Top Exclusion Reasons
| Model | Exclusion Reason | Count | Share Within Model Exclusions |
| --- | --- | ---: | ---: |
| SDE | NO_MATCH_WITHIN_HORIZON | 4873 | 0.918914 |
| SDE | UNRESOLVED_MISSING_POST_GAP | 240 | 0.045257 |
| SDE | UNRESOLVED_INCIDENT_FILTER | 148 | 0.027909 |
| SDE | WEATHER_SURVIVAL_STOP | 42 | 0.007920 |
| ML | NO_MATCH_WITHIN_HORIZON | 1600 | 0.970285 |
| ML | UNRESOLVED_MISSING_POST_GAP | 25 | 0.015161 |
| ML | UNRESOLVED_INCIDENT_FILTER | 21 | 0.012735 |
| ML | UNRESOLVED_MISSING_PRE_GAP | 2 | 0.001213 |
| ML | WEATHER_SURVIVAL_STOP | 1 | 0.000606 |

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
- Summary CSV: data_lake/reports/no_source_year_percent_conservative_v1/sde_ml_comparison_summary_2022_2025_racewise_no_source_year_percent_conservative_v1.csv
- By-year CSV: data_lake/reports/no_source_year_percent_conservative_v1/sde_ml_comparison_by_year_2022_2025_racewise_no_source_year_percent_conservative_v1.csv
