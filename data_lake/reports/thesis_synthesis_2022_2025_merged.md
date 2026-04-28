# Thesis Final Synthesis (2022_2025_merged)

## Executive Summary
- Integrated gate decision: **NO_GO** (confidence=LOW; core validity gate failed in D).
- Batch constrained policy: precision=0.803150, recall=0.105631, positive_rate=0.009863.
- Latency: p95=10.268393 ms, availability=100.00% (operational target p95<10.0 ms).
- MOA second explainability method: fidelity_accuracy=0.777219, fidelity_f1=0.229366.

## Claim Matrix
| Model | Actionable | Scored | Precision | TP | FP | Scored Rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SDE Heuristic | 6323 | 1020 | 0.734314 | 749 | 271 | 0.161316 |
| Batch ML (Constrained) | 1016 | 595 | 0.942857 | 561 | 34 | 0.585630 |
| Batch ML (Best Threshold) | 33386 | 3734 | 0.843867 | 3151 | 583 | 0.111843 |
| MOA Streaming | 6453 | 1430 | 0.904196 | 1293 | 137 | 0.221602 |

## Correctness Audit
- Checks passed: 11
- Checks failed: 3

| Check | Status | Note |
| --- | --- | --- |
| phase_b_sde_precision_consistency | PASS | recomputed=0.734313725490, phase_b=0.734313725490 |
| phase_b_ml_precision_consistency | PASS | recomputed=0.942857142857, phase_b=0.942857142857 |
| phase_c_best_threshold_selected | PASS | selected_threshold=0.050000 |
| phase_c_best_scored_consistency | PASS | best_comparator_scored=3734, phase_c_scored=3734 |
| phase_d_raw_ece_consistency | PASS | bins=0.261433769837, summary=0.261433769837 |
| phase_d_calibrated_ece_consistency | PASS | bins=0.006690241810, summary=0.006690241810 |
| phase_d_raw_mce_consistency | PASS | bins=0.561550090379, summary=0.561550090379 |
| phase_d_calibrated_mce_consistency | PASS | bins=0.099172619710, summary=0.099172619710 |
| phase_g_p95_consistency | PASS | summary=10.268393499427, details=10.268393499427 |
| integrated_gate_go | FAIL | phase_h_status=NO_GO |
| phase_f_parity_gate | PASS | phase_f_status=PASS |
| split_integrity_overall | FAIL | phase_j_split_status=FAIL |
| new_artifacts_presence | FAIL | missing=data_lake/reports/paper_fig2_calibration_reliability_2022_2025_merged.pdf;data_lake/reports/paper_fig3_calibration_gap_2022_2025_merged.pdf;data_lake/reports/paper_fig4_latency_by_year_p95_2022_2025_merged.pdf;data_lake/reports/paper_fig5_latency_components_2022_2025_merged.pdf |
| moa_permutation_rows_nonzero | PASS | rows_used=123213, unknown_rows=923 |
## Interpretation Notes
- Batch best-threshold policy maximizes scored recovery under precision-floor constraints and should be used for competitive-capability comparisons.
- Batch constrained policy should be used for deployment-readiness claims (calibrated conservative actions).
- MOA explainability is proxy-based: surrogate SHAP plus temporal permutation importance; treat as behavioral attribution, not internal-model attribution.
- Split-integrity fold-count mismatch remains a visible residual risk in current artifacts and should be discussed explicitly in thesis limitations.

## Produced by
- ml_pipeline/build_thesis_synthesis.py
