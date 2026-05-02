# 3-way Compact Comparison

Years: [2022, 2023, 2024, 2025]
Season tag: merged

| Paradigm | Mode | Actionable | Scored | Excluded | TP | FP | Precision | Eval instances | Accuracy % | Kappa % |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SDE | full_h2_actionable_one_to_one | 6323 | 1020 | 5303 | 749 | 271 | 0.734314 | N/A | N/A | N/A |
| ML_BATCH | full_h2_actionable_one_to_one | 495 | 311 | 184 | 300 | 11 | 0.964630 | N/A | N/A | N/A |
| MOA_ARF_DECISION | full_h2_actionable_one_to_one | 7119 | 1687 | 5432 | 1482 | 205 | 0.878483 | N/A | N/A | N/A |
| MOA_ARF | prequential_stream_baseline | N/A | N/A | N/A | N/A | N/A | N/A | 124136 | 89.416446 | 16.993183 |

## Caveats
- SDE, batch ML, and MOA decision rows are directly comparable under the fixed H=2 comparator contract.
- MOA prequential row remains useful as a stream quality baseline but is not itself a decision-level comparator metric.
- SHAP for MOA is surrogate-based and must be interpreted as behavior approximation, not MOA internal attribution.
