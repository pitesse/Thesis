# Thesis Final Synthesis (2022_2025_merged)

## Executive Summary
- Integrated gate decision: **NO_GO** (confidence=LOW; core validity gate failed in D).
- Batch constrained policy: precision=0.803150, recall=0.105631, positive_rate=0.009863.
- Latency: p95=10.268393 ms, availability=100.00% (operational target p95<10.0 ms).
- MOA second explainability method: fidelity_accuracy=0.776772, fidelity_f1=0.230091.
- OOF discrimination (calibrated PR-AUC/AP): pretrain=0.394339, racewise=0.371696.
- MOA surrogate selection: rf_300_d12 (RandomForestClassifier), fidelity_f1=0.230091.

## Claim Matrix
| Model | Actionable | Scored | Precision | TP | FP | Scored Rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SDE Heuristic | 6323 | 1020 | 0.734314 | 749 | 271 | 0.161316 |
| Batch ML (Constrained) | 1016 | 595 | 0.942857 | 561 | 34 | 0.585630 |
| Batch ML (Best Threshold) | 33386 | 3734 | 0.843867 | 3151 | 583 | 0.111843 |
| MOA Streaming | 6453 | 1430 | 0.904196 | 1293 | 137 | 0.221602 |

## Discrimination and Explainability Addendum
| Protocol | Score Type | PR-AUC (AP) | PR-AUC (Trapz) | Prevalence | Rows |
| --- | --- | ---: | ---: | ---: | ---: |
| ml_pretrain | calibrated_proba | 0.394339 | 0.396429 | 0.074989 | 103015 |
| ml_pretrain | raw_proba | 0.402136 | 0.402086 | 0.074989 | 103015 |
| ml_racewise | calibrated_proba | 0.371696 | 0.374134 | 0.067011 | 123129 |
| ml_racewise | raw_proba | 0.381090 | 0.381042 | 0.067011 | 123129 |

| MOA Surrogate Rank | Model ID | Family | F1 | Accuracy | Precision | Recall | Selected |
| ---: | --- | --- | ---: | ---: | ---: | ---: | --- |
| 1 | rf_300_d12 | RandomForestClassifier | 0.230091 | 0.776772 | 0.140417 | 0.636716 | yes |
| 2 | rf_100_d10 | RandomForestClassifier | 0.207288 | 0.728118 | 0.122329 | 0.678544 | no |
| 3 | extra_300_d12 | ExtraTreesClassifier | 0.200895 | 0.681045 | 0.115623 | 0.765298 | no |
| 4 | logreg_balanced | LogisticRegression+Imputer+Scaler | 0.153545 | 0.613440 | 0.086721 | 0.669249 | no |
| 5 | hgb_400_d8 | HistGradientBoostingClassifier | 0.097942 | 0.948423 | 0.584746 | 0.053447 | no |

PR artifacts:
- `pr_metrics_2022_2025.csv`
- `pr_operating_points_2022_2025.csv`
- `pr_curves_overall_2022_2025.pdf`
- `pr_curves_by_year_2022_2025.pdf`
- `pr_curves_panel_2022_2025.pdf`

## Correctness Audit
- Checks passed: 18
- Checks failed: 2

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
| pr_overall_rows_presence | PASS | present_pairs=[('ml_pretrain', 'calibrated_proba'), ('ml_pretrain', 'raw_proba'), ('ml_racewise', 'calibrated_proba'), ('ml_racewise', 'raw_proba')] |
| pr_ap_expected_scale | PASS | ml_pretrain_ap=0.394339, ml_racewise_ap=0.371696 |
| pr_protocol_ordering | PASS | ml_pretrain_ap=0.394339, ml_racewise_ap=0.371696 |
| pr_operating_points_presence | PASS | protocols_with_constrained_median=['ml_pretrain', 'ml_racewise'] |
| moa_surrogate_single_selection | PASS | selected_rows=1 |
| moa_surrogate_selection_consistency | PASS | sweep_selected=rf_300_d12, permutation_selected=rf_300_d12 |
| new_artifacts_presence | PASS | missing= |
| moa_permutation_rows_nonzero | PASS | rows_used=123213, unknown_rows=923 |
## Interpretation Notes
- Batch best-threshold policy maximizes scored recovery under precision-floor constraints and should be used for competitive-capability comparisons.
- Batch constrained policy should be used for deployment-readiness claims (calibrated conservative actions).
- MOA explainability is proxy-based: surrogate SHAP plus temporal permutation importance; treat as behavioral attribution, not internal-model attribution.
- Split-integrity fold-count mismatch remains a visible residual risk in current artifacts and should be discussed explicitly in thesis limitations.

## Produced by
- ml_pipeline/build_thesis_synthesis.py
