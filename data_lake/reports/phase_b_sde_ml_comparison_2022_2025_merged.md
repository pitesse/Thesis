# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-04-16T11:55:58.185550+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Phase B summary: data_lake/reports/phase_b_significance_summary_2022_2025_merged.csv
- Phase B tests: data_lake/reports/phase_b_significance_tests_2022_2025_merged.csv
- SDE comparator: data_lake/reports/heuristic_comparator_2022_2025_merged.csv
- ML comparator: data_lake/reports/ml_comparator_2022_2025_merged.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 6323 | 1020 | 5303 | 749 | 271 | 0.734314 | [0.706365, 0.760504] |
| ML | 847 | 522 | 325 | 501 | 21 | 0.959770 | [0.939284, 0.973539] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | 10.692854 | <1e-16 (underflow) | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 0.000000 | 1 | paired only on shared race driver lap keys, overlap_n=89, discordant=0 |
| mcnemar_exact | overlap_scored_keys_only | 0.000000 | 1 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=0, sde_failure_ml_success=0 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): 0.225456
- Scored-row delta (ML - SDE): -498
- Actionable-row delta (ML - SDE): -5476
- Scored ratio (ML / SDE): 0.511765
- Actionable ratio (ML / SDE): 0.133955
- Overlap scored keys: 89
- Overlap ratio vs SDE scored: 0.087255, vs ML scored: 0.170498

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 155 | 0.645161 | 5 | 1.000000 | 0.354839 | -150 |
| 2023 | 226 | 0.721239 | 57 | 1.000000 | 0.278761 | -169 |
| 2024 | 299 | 0.759197 | 217 | 0.953917 | 0.194720 | -82 |
| 2025 | 340 | 0.761765 | 243 | 0.954733 | 0.192968 | -97 |

## Top Exclusion Reasons
| Model | Exclusion Reason | Count | Share Within Model Exclusions |
| --- | --- | ---: | ---: |
| SDE | NO_MATCH_WITHIN_HORIZON | 4873 | 0.918914 |
| SDE | UNRESOLVED_MISSING_POST_GAP | 240 | 0.045257 |
| SDE | UNRESOLVED_INCIDENT_FILTER | 148 | 0.027909 |
| SDE | WEATHER_SURVIVAL_STOP | 42 | 0.007920 |
| ML | NO_MATCH_WITHIN_HORIZON | 307 | 0.944615 |
| ML | UNRESOLVED_MISSING_POST_GAP | 11 | 0.033846 |
| ML | UNRESOLVED_INCIDENT_FILTER | 5 | 0.015385 |
| ML | WEATHER_SURVIVAL_STOP | 2 | 0.006154 |

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
- Summary CSV: data_lake/reports/phase_b_sde_ml_comparison_summary_2022_2025_merged.csv
- By-year CSV: data_lake/reports/phase_b_sde_ml_comparison_by_year_2022_2025_merged.csv
