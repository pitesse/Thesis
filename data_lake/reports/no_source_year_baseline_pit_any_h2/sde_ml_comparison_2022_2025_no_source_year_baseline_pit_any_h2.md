# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-05-06T14:06:27.265318+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Significance summary: data_lake/reports/no_source_year_baseline_pit_any_h2/significance_summary_2022_2025_no_source_year_baseline_pit_any_h2.csv
- Significance tests: data_lake/reports/no_source_year_baseline_pit_any_h2/significance_tests_2022_2025_no_source_year_baseline_pit_any_h2.csv
- SDE comparator: data_lake/reports/no_source_year_baseline_pit_any_h2/heuristic_comparator_2022_2025_no_source_year_baseline_pit_any_h2.csv
- ML comparator: data_lake/reports/no_source_year_baseline_pit_any_h2/ml_comparator_2022_2025_no_source_year_baseline_pit_any_h2.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 5453 | 5453 | 0 | 839 | 4614 | 0.153860 | [0.144528, 0.163680] |
| ML | 579 | 579 | 0 | 110 | 469 | 0.189983 | [0.160113, 0.223939] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | 2.269718 | 0.0232247 | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 4.000000 | 0.0455003 | paired only on shared race driver lap keys, overlap_n=87, discordant=9 |
| mcnemar_exact | overlap_scored_keys_only | 9.000000 | 0.0390625 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=8, sde_failure_ml_success=1 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): 0.036122
- Scored-row delta (ML - SDE): -4874
- Actionable-row delta (ML - SDE): -4874
- Scored ratio (ML / SDE): 0.106180
- Actionable ratio (ML / SDE): 0.106180
- Overlap scored keys: 87
- Overlap ratio vs SDE scored: 0.015955, vs ML scored: 0.150259

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 1212 | 0.169967 | N/A | N/A | N/A | N/A |
| 2023 | 1223 | 0.160262 | 181 | 0.176796 | 0.016534 | -1042 |
| 2024 | 1389 | 0.163427 | 220 | 0.159091 | -0.004336 | -1169 |
| 2025 | 1629 | 0.128913 | 178 | 0.241573 | 0.112660 | -1451 |

## Top Exclusion Reasons
No excluded rows found in comparator artifacts.

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
- Summary CSV: data_lake/reports/no_source_year_baseline_pit_any_h2/sde_ml_comparison_summary_2022_2025_no_source_year_baseline_pit_any_h2.csv
- By-year CSV: data_lake/reports/no_source_year_baseline_pit_any_h2/sde_ml_comparison_by_year_2022_2025_no_source_year_baseline_pit_any_h2.csv
