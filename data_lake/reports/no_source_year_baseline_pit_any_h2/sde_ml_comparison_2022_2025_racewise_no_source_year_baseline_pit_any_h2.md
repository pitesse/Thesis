# Dedicated SDE vs ML Comparison Report

Generated at (UTC): 2026-05-05T18:30:15.558014+00:00

## Scope
- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.
- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.
- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.

## Inputs
- Significance summary: data_lake/reports/no_source_year_baseline_pit_any_h2/significance_summary_2022_2025_racewise_no_source_year_baseline_pit_any_h2.csv
- Significance tests: data_lake/reports/no_source_year_baseline_pit_any_h2/significance_tests_2022_2025_racewise_no_source_year_baseline_pit_any_h2.csv
- SDE comparator: data_lake/reports/no_source_year_baseline_pit_any_h2/heuristic_comparator_2022_2025_racewise_no_source_year_baseline_pit_any_h2.csv
- ML comparator: data_lake/reports/no_source_year_baseline_pit_any_h2/ml_comparator_2022_2025_racewise_no_source_year_baseline_pit_any_h2.csv

## Headline Comparison
| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 5453 | 5453 | 0 | 839 | 4614 | 0.153860 | [0.144528, 0.163680] |
| ML | 806 | 806 | 0 | 149 | 657 | 0.184864 | [0.159581, 0.213136] |

## Statistical Evidence
| Test | Pairing Scope | Statistic | P-value | Note |
| --- | --- | ---: | ---: | --- |
| two_proportion_z | independent_scored_rows | 2.253306 | 0.0242399 | tests precision difference on scored rows, ignores pairing |
| mcnemar_cc | overlap_scored_keys_only | 6.050000 | 0.0139063 | paired only on shared race driver lap keys, overlap_n=162, discordant=20 |
| mcnemar_exact | overlap_scored_keys_only | 20.000000 | 0.0118179 | exact binomial mcnemar p value on discordant overlap pairs, sde_success_ml_failure=16, sde_failure_ml_success=4 |

## Coverage and Overlap Diagnostics
- Precision delta (ML - SDE): 0.031003
- Scored-row delta (ML - SDE): -4647
- Actionable-row delta (ML - SDE): -4647
- Scored ratio (ML / SDE): 0.147809
- Actionable ratio (ML / SDE): 0.147809
- Overlap scored keys: 162
- Overlap ratio vs SDE scored: 0.029708, vs ML scored: 0.200993

## Per-Year Comparison
| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | 1213 | 0.169827 | 130 | 0.238462 | 0.068635 | -1083 |
| 2023 | 1219 | 0.160788 | 172 | 0.209302 | 0.048515 | -1047 |
| 2024 | 1399 | 0.163688 | 301 | 0.122924 | -0.040765 | -1098 |
| 2025 | 1622 | 0.128237 | 203 | 0.221675 | 0.093438 | -1419 |

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
- Summary CSV: data_lake/reports/no_source_year_baseline_pit_any_h2/sde_ml_comparison_summary_2022_2025_racewise_no_source_year_baseline_pit_any_h2.csv
- By-year CSV: data_lake/reports/no_source_year_baseline_pit_any_h2/sde_ml_comparison_by_year_2022_2025_racewise_no_source_year_baseline_pit_any_h2.csv
