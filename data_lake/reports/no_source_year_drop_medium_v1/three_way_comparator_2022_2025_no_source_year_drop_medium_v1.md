# 3-way Compact Comparison

Years: [2022, 2023, 2024, 2025]
Season tag: season

| Paradigm | Mode | Actionable | Scored | Excluded | TP | FP | Precision | Eval instances | Accuracy % | Kappa % |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SDE | full_h2_actionable_one_to_one | 5453 | 486 | 4967 | 305 | 181 | 0.627572 | N/A | N/A | N/A |
| ML_BATCH | full_h2_actionable_one_to_one | 506 | 66 | 440 | 46 | 20 | 0.696970 | N/A | N/A | N/A |
| MOA_ARF_DECISION | full_h2_actionable_one_to_one | 1418 | 305 | 1113 | 221 | 84 | 0.724590 | N/A | N/A | N/A |
| MOA_ARF | prequential_stream_baseline | N/A | N/A | N/A | N/A | N/A | N/A | 93623 | 95.966803 | 10.299988 |

## Caveats
- SDE, batch ML, and MOA decision rows are directly comparable under the fixed H=2 comparator contract.
- MOA prequential row remains useful as a stream quality baseline but is not itself a decision-level comparator metric.
- SHAP for MOA is surrogate-based and must be interpreted as behavior approximation, not MOA internal attribution.
