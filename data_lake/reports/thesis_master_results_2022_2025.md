# Thesis Master Results Table (2022-2025)

Generated at (UTC): 2026-04-30T10:32:31.987799+00:00

No new training runs were executed for this report; all numbers are computed from existing artifacts in `data_lake/reports`.

## 1) Master Comparison Table

| Approach | Actionable | Scored | TP | FP | Precision | Scored/Actionable Coverage | Protocol |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SDE | 6,323 | 1,020 | 749 | 271 | 0.734314 | 0.161316 | Deterministic stream baseline, fixed H=2 comparator |
| ML-pretrain-base | 1,016 | 595 | 561 | 34 | 0.942857 | 0.585630 | Batch ML, expanding_race (year warmup before race-level expansion) |
| ML-pretrain-extended | 33,386 | 3,734 | 3,151 | 583 | 0.843867 | 0.111843 | Batch ML, expanding_race + threshold frontier selection |
| ML-racewise-base | 2,400 | 653 | 597 | 56 | 0.914242 | 0.272083 | Batch ML, expanding_race_sequential (no year pretrain) |
| ML-racewise-extended | 36,873 | 3,913 | 3,264 | 649 | 0.834143 | 0.106121 | Batch ML, expanding_race_sequential + threshold frontier selection |
| MOA | 6,453 | 1,430 | 1,293 | 137 | 0.904196 | 0.221602 | Streaming ML (MOA ARF) decision-mapped comparator |

Master table artifacts used:
- `SDE`: `data_lake/reports/heuristic_comparator_2022_2025_merged.csv`
- `ML-pretrain-base`: `data_lake/reports/ml_comparator_2022_2025_merged.csv`
- `ML-pretrain-extended`: `data_lake/reports/ml_comparator_best_threshold_2022_2025_merged.csv`
- `ML-racewise-base`: `data_lake/reports/ml_comparator_2022_2025_racewise.csv`
- `ML-racewise-extended`: `data_lake/reports/ml_comparator_best_threshold_2022_2025_racewise.csv`
- `MOA`: `data_lake/reports/moa_comparator_2022_2025_merged.csv`

## 2) How This Data Was Produced, and Why It Is Methodologically Valid

Data generation chain (already executed):
- Batch ML: OOF probabilities from `train_model.py` with leakage-safe grouped structures, then comparator mapping under fixed horizon `H=2` and one-to-one actionable matching.
- Extended reachability variants: threshold frontier sweep from OOF probabilities, then comparator rebuild at selected threshold (no retraining).
- MOA: ARF prequential run on exported MOA dataset, decoded predictions mapped into the same comparator contract used by SDE/ML.

Academic and methodological backing used in this pipeline:
- Split/leakage rigor: grouped race-level separation and temporal constraints (Roberts 2017, Brookshire 2024).
- Imbalance-aware objective and precision-centric analysis: class-weighted framing and PR-oriented interpretation (Elkan 2001, Saito and Rehmsmeier 2015, Davis and Goadrich 2006).
- Significance reporting: two-proportion z test as primary evidence and overlap McNemar as paired sensitivity (Dietterich 1998, Walters 2022).
- Calibration reliability and deployment validity gates: Brier/calibration policy checks, train-serve parity, latency/availability checks before operational claims.

Key inferential evidence (SDE vs ML pretrain-base comparator):
- SDE precision: 0.734314, ML precision: 0.942857, delta: 0.208543
- Two-proportion z statistic: 10.328940, p-value: <1e-16 (underflow)
- McNemar cc statistic: 0.500000, p-value: 0.479500
- Source files: `data_lake/reports/significance_summary_2022_2025_merged.csv`, `data_lake/reports/significance_tests_2022_2025_merged.csv`

## 3) Batch ML Protocol Differentiation (Pretrain-Year vs Pure Racewise)

| Variant | OOF Rows | Split Protocol | Fold Count | Test Years Present | Baseline Threshold |
| --- | ---: | --- | ---: | --- | ---: |
| ML-pretrain-base | 103,015 | expanding_race | 74 | 2023, 2024, 2025 | 0.50 |
| ML-racewise-base | 123,129 | expanding_race_sequential | 95 | 2022, 2023, 2024, 2025 | 0.50 |

Protocol note: on current artifacts, pretrain and racewise OOF predictions are identical on 2023-2025 rows; racewise extends evaluation coverage by adding 2022.

Extended reachability (threshold frontier, no retraining):
| Variant | Selected Threshold | Reference Threshold | Selected Precision | Reference Precision | Sweep Report |
| --- | ---: | ---: | ---: | ---: | --- |
| ML-pretrain-extended | 0.050 | 0.100 | 0.843867 | 0.877658 | `data_lake/reports/threshold_frontier_report_2022_2025_merged.txt` |
| ML-racewise-extended | N/A | N/A | 0.834143 | 0.914242 | `not available (no racewise sweep artifact)` |

## 3.1) OOF Discrimination Evidence (PR Curves and PR-AUC)

This section reports **OOF row-level classification discrimination** from probability outputs (`target_y` vs model probabilities).
It is methodologically distinct from comparator precision tables, which evaluate decision-level matching under the fixed `H=2` contract.

| Protocol | Score Type | PR-AUC (AP) | PR-AUC (Trapz) | Prevalence | Rows | Positives |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| ml_pretrain | calibrated_proba | 0.394339 | 0.396429 | 0.074989 | 103,015 | 7,725 |
| ml_pretrain | raw_proba | 0.402136 | 0.402086 | 0.074989 | 103,015 | 7,725 |
| ml_racewise | calibrated_proba | 0.371696 | 0.374134 | 0.067011 | 123,129 | 8,251 |
| ml_racewise | raw_proba | 0.381090 | 0.381042 | 0.067011 | 123,129 | 8,251 |

Per-year PR-AUC (calibrated probabilities):
| Protocol | Year | PR-AUC (AP) | Prevalence | Rows | Positives |
| --- | ---: | ---: | ---: | ---: | ---: |
| ml_pretrain | 2023 | 0.205870 | 0.053499 | 29,907 | 1,600 |
| ml_pretrain | 2024 | 0.400268 | 0.078612 | 33,239 | 2,613 |
| ml_pretrain | 2025 | 0.465371 | 0.088088 | 39,869 | 3,512 |
| ml_racewise | 2022 | 0.060514 | 0.026151 | 20,114 | 526 |
| ml_racewise | 2023 | 0.205870 | 0.053499 | 29,907 | 1,600 |
| ml_racewise | 2024 | 0.400268 | 0.078612 | 33,239 | 2,613 |
| ml_racewise | 2025 | 0.465371 | 0.088088 | 39,869 | 3,512 |

- PR metrics artifact: `data_lake/reports/pr_metrics_2022_2025.csv`
- PR operating points artifact: `data_lake/reports/pr_operating_points_2022_2025.csv`

![PR curves overall](pr_curves_overall_2022_2025.png)
![PR curves by year](pr_curves_by_year_2022_2025.png)
![PR curves panel](pr_curves_panel_2022_2025.png)

## 4) Per-Year Comparison Across the Three Paradigms (plus Batch ML variants)

| Year | Method | Actionable | Scored | TP | FP | Precision | Coverage |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022 | ML-racewise-base | 1,384 | 58 | 36 | 22 | 0.620690 | 0.041908 |
| 2022 | ML-racewise-extended | 3,487 | 179 | 113 | 66 | 0.631285 | 0.051334 |
| 2022 | MOA | 435 | 107 | 95 | 12 | 0.887850 | 0.245977 |
| 2022 | SDE | 1,398 | 155 | 100 | 55 | 0.645161 | 0.110873 |
| 2023 | ML-pretrain-base | 91 | 52 | 51 | 1 | 0.980769 | 0.571429 |
| 2023 | ML-pretrain-extended | 5,651 | 528 | 425 | 103 | 0.804924 | 0.093435 |
| 2023 | ML-racewise-base | 91 | 52 | 51 | 1 | 0.980769 | 0.571429 |
| 2023 | ML-racewise-extended | 5,651 | 528 | 425 | 103 | 0.804924 | 0.093435 |
| 2023 | MOA | 1,407 | 285 | 256 | 29 | 0.898246 | 0.202559 |
| 2023 | SDE | 1,572 | 226 | 163 | 63 | 0.721239 | 0.143766 |
| 2024 | ML-pretrain-base | 235 | 155 | 148 | 7 | 0.954839 | 0.659574 |
| 2024 | ML-pretrain-extended | 12,520 | 1,324 | 1,117 | 207 | 0.843656 | 0.105751 |
| 2024 | ML-racewise-base | 235 | 155 | 148 | 7 | 0.954839 | 0.659574 |
| 2024 | ML-racewise-extended | 12,520 | 1,324 | 1,117 | 207 | 0.843656 | 0.105751 |
| 2024 | MOA | 1,584 | 352 | 325 | 27 | 0.923295 | 0.222222 |
| 2024 | SDE | 1,647 | 299 | 227 | 72 | 0.759197 | 0.181542 |
| 2025 | ML-pretrain-base | 690 | 388 | 362 | 26 | 0.932990 | 0.562319 |
| 2025 | ML-pretrain-extended | 15,215 | 1,882 | 1,609 | 273 | 0.854942 | 0.123694 |
| 2025 | ML-racewise-base | 690 | 388 | 362 | 26 | 0.932990 | 0.562319 |
| 2025 | ML-racewise-extended | 15,215 | 1,882 | 1,609 | 273 | 0.854942 | 0.123694 |
| 2025 | MOA | 3,027 | 686 | 617 | 69 | 0.899417 | 0.226627 |
| 2025 | SDE | 1,706 | 340 | 259 | 81 | 0.761765 | 0.199297 |

## 4.1) Per-Race Access (Raw Decision-Level Comparator Rows)

| Method | Unique Races Covered | Raw Per-Race Artifact |
| --- | ---: | --- |
| SDE | 96 | `data_lake/reports/heuristic_comparator_2022_2025_merged.csv` |
| ML-pretrain-base | 60 | `data_lake/reports/ml_comparator_2022_2025_merged.csv` |
| ML-pretrain-extended | 74 | `data_lake/reports/ml_comparator_best_threshold_2022_2025_merged.csv` |
| ML-racewise-base | 73 | `data_lake/reports/ml_comparator_2022_2025_racewise.csv` |
| ML-racewise-extended | 93 | `data_lake/reports/ml_comparator_best_threshold_2022_2025_racewise.csv` |
| MOA | 95 | `data_lake/reports/moa_comparator_2022_2025_merged.csv` |

## 5) MOA Additional Baseline Context (Prequential Stream Metrics)

| Metric | Value | Source |
| --- | ---: | --- |
| EvaluatePrequential instances | 124,136 | `data_lake/reports/moa_arf_summary_2022_2025_merged.csv` |
| Final accuracy (%) | 89.447058 | `data_lake/reports/moa_arf_summary_2022_2025_merged.csv` |
| Final kappa (%) | 13.696910 | `data_lake/reports/moa_arf_summary_2022_2025_merged.csv` |

Note: Prequential metrics are stream-learning diagnostics and are reported separately from comparator-precision metrics.

## 6) SHAP and Explainability Analysis Across Models

Interpretability availability by paradigm:
- SDE: deterministic rule system, interpretable by rule logic and comparator diagnostics, SHAP not applicable.
- Batch ML: direct TreeSHAP on serving-bundle gradient-boosted model.
- MOA: surrogate explainability (SHAP proxy + temporal permutation) on decoded MOA decisions, with explicit fidelity caveat.

MOA surrogate model sweep (fixed holdout protocol):
| Rank | Model ID | Family | F1 | Accuracy | Precision | Recall | Balanced Accuracy | Selected |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | rf_300_d12 | RandomForestClassifier | 0.230091 | 0.776772 | 0.140417 | 0.636716 | 0.710615 | yes |
| 2 | rf_100_d10 | RandomForestClassifier | 0.207288 | 0.728118 | 0.122329 | 0.678544 | 0.704701 | no |
| 3 | extra_300_d12 | ExtraTreesClassifier | 0.200895 | 0.681045 | 0.115623 | 0.765298 | 0.720843 | no |
| 4 | logreg_balanced | LogisticRegression+Imputer+Scaler | 0.153545 | 0.613440 | 0.086721 | 0.669249 | 0.639802 | no |
| 5 | hgb_400_d8 | HistGradientBoostingClassifier | 0.097942 | 0.948423 | 0.584746 | 0.053447 | 0.525674 | no |

- Surrogate sweep artifact: `data_lake/reports/moa_surrogate_model_sweep.csv`

Batch ML SHAP top features (`shap_feature_importance.csv`):
| Rank | Feature | Mean abs SHAP |
| --- | --- | ---: |
| 1 | _source_year | 0.419389 |
| 2 | tire_life_ratio | 0.406036 |
| 3 | trackTemp | 0.251564 |
| 4 | humidity | 0.251033 |
| 5 | lapTime | 0.245820 |
| 6 | airTemp | 0.207093 |
| 7 | speedTrap | 0.156061 |
| 8 | tyreLife | 0.150638 |
| 9 | position | 0.140383 |
| 10 | gap_to_physical_car | 0.129288 |

MOA surrogate SHAP proxy top features (`moa_shap_proxy_feature_importance.csv`):
| Rank | Feature | Mean abs SHAP |
| --- | --- | ---: |
| 1 | _source_year | 0.051279 |
| 2 | speedTrap | 0.034801 |
| 3 | gapBehind | 0.032626 |
| 4 | gapAhead | 0.029397 |
| 5 | trackTemp | 0.021724 |
| 6 | tire_life_ratio | 0.019810 |
| 7 | pace_drop_ratio | 0.018650 |
| 8 | gap_to_physical_car | 0.018149 |
| 9 | tyreLife | 0.016020 |
| 10 | pitLoss | 0.013977 |

MOA temporal permutation top features (`moa_temporal_permutation_global.csv`):
| Rank | Feature | Mean F1 Drop |
| --- | --- | ---: |
| 1 | _source_year | 0.035963 |
| 2 | speedTrap | 0.016138 |
| 3 | gapBehind | 0.015116 |
| 4 | gap_to_physical_car | 0.011480 |
| 5 | trackTemp | 0.011252 |
| 6 | gapAhead | 0.009865 |
| 7 | tyreLife | 0.009475 |
| 8 | lapTime | 0.008448 |
| 9 | pace_drop_ratio | 0.007115 |
| 10 | tire_life_ratio | 0.006529 |

MOA explainability fidelity diagnostics:
- SHAP proxy fidelity accuracy: 0.776772, fidelity F1: 0.230091, rows used: 123,213
- Temporal permutation surrogate fidelity accuracy: 0.776772, fidelity F1: 0.230091, rows used: 123,213
- Top-10 overlap between MOA SHAP proxy and MOA permutation: _source_year, gapAhead, gapBehind, gap_to_physical_car, pace_drop_ratio, speedTrap, tire_life_ratio, trackTemp, tyreLife

SHAP visual artifacts:

Batch ML SHAP:
![Batch ML SHAP global bar](shap_global_bar.png)
![Batch ML SHAP beeswarm](shap_beeswarm.png)
![Batch ML SHAP dependence source year](shap_dependence__source_year.png)

MOA SHAP proxy:
![MOA SHAP proxy global bar](moa_shap_proxy_global_bar.png)
![MOA SHAP proxy beeswarm](moa_shap_proxy_beeswarm.png)
![MOA SHAP proxy dependence source year](moa_shap_proxy_dependence__source_year.png)

MOA temporal permutation heatmap:
- [moa_temporal_permutation_heatmap.pdf](moa_temporal_permutation_heatmap.pdf)

## 6.1) `_source_year` Deployment Note (No Retraining)

`_source_year` appears as a top-ranked feature in both Batch SHAP and MOA surrogate explainability artifacts.
The current pipeline injects this feature both offline and online, and no retraining is required for this note.

Technical validation from existing code and artifacts:
- Offline feature construction: `_source_year` is generated in `ml_pipeline/lib/data_preparation.py` and persisted in datasets/OOF artifacts.
- Online serving path: `_source_year` is derived from race metadata in `ml_pipeline/lib/live_kafka_inference.py` (`_source_year_from_race`).
- Train-serve parity gate on current artifacts: `PASS` (value=1.000000, threshold=N/A).
- Tree-based serving models accept unseen numeric year values at inference without invalidating rows; decisions follow learned split thresholds.
- Future hardening option (deferred): replace absolute year with `years_since_2022` to make extrapolation semantics explicit.

## 7) Deployment-Readiness and Validity Gates (Current Artifact Snapshot)

| Test ID | Name | Status | Metric | Value | Threshold | Artifact |
| --- | --- | --- | --- | ---: | ---: | --- |
| B1 | ML precision delta vs SDE | PASS | precision_delta | 0.208543 | 0.000000 | data_lake/reports/significance_summary_2022_2025_merged.csv |
| B2 | Two-proportion z significance | PASS | p_value | 0.000000 | 0.050000 | data_lake/reports/significance_tests_2022_2025_merged.csv |
| C1 | Reference precision floor | PASS | reference_precision | 0.877658 | 0.734314 | data_lake/reports/threshold_frontier_2022_2025_merged.csv |
| C2 | Lookahead no-match dominance | PASS | no_match_rate | 0.968374 | 0.900000 | data_lake/reports/threshold_frontier_2022_2025_merged.csv |
| D1 | Constrained precision | PASS | constrained_precision | 0.803150 | 0.600000 | data_lake/reports/calibration_policy_summary_2022_2025_merged.csv |
| D2 | Precision-floor reachability | FAIL | reachability_ratio | 0.899672 | 0.900000 | data_lake/reports/calibration_policy_summary_2022_2025_merged.csv |
| D3 | Fallback rate | PASS | fallback_rate | 0.000000 | 0.100000 | data_lake/reports/calibration_policy_summary_2022_2025_merged.csv |
| F1 | Training-serving parity gate | PASS | feature_parity_overall_gate | 1.000000 | 1.000000 | data_lake/reports/feature_parity_summary_2022_2025_merged.csv |
| G1 | Latency gate | PASS | latency_p95_total_ms | 10.268393 | 500.000000 | data_lake/reports/live_latency_summary_2022_2025_merged.csv |
| G2 | Availability gate | PASS | availability_pct | 100.000000 | 99.000000 | data_lake/reports/live_latency_summary_2022_2025_merged.csv |
| H1 | Integrated deployment decision | NO_GO | integrated_gate_decision | N/A | N/A | data_lake/reports/integrated_gate_2022_2025_merged.csv |
| J1 | Split-integrity gate | FAIL | split_integrity_overall | 0.000000 | 1.000000 | data_lake/reports/split_integrity_summary_2022_2025_merged.csv |
| J2 | Comparator-invariance gate | PASS | comparator_invariance_overall | 1.000000 | 1.000000 | data_lake/reports/comparator_invariance_summary_2022_2025_merged.csv |

Caveat on split-integrity: `oof_fold_count_match` assumes grouped-kfold fold cardinality and will fail for expanding protocols with many sequential test folds, despite race-overlap checks passing.

## 8) Professor-Friendly Naming Guide

| Current Generator / Report Family | Simple Name for Discussion | One-Sentence Explanation |
| --- | --- | --- |
| `significance*`, `sde_ml_comparison*` | Comparative Precision Significance | Checks whether ML truly beats SDE under the same decision contract and whether that gain is statistically credible. |
| `threshold_frontier*` | Threshold Reachability Frontier | Shows precision-versus-coverage tradeoff when relaxing threshold to increase actionable reach. |
| `calibration_policy*` | Calibration and Policy Reliability | Verifies probability quality and constrained-policy stability under precision-floor constraints. |
| `feature_parity*` | Train-Serve Feature Parity | Confirms online feature pipeline matches offline training features to avoid hidden skew. |
| `live_latency*`, `live_availability*`, `live_overhead*` | Real-Time Runtime Feasibility | Quantifies latency and availability to verify operational viability. |
| `integrated_gate*` | Integrated Go/No-Go Decision | Aggregates evidence from significance/threshold/calibration/parity/runtime into one deployment-readiness decision. |
| `split_integrity*`, `comparator_invariance*` | Protocol Closure Audits | Final audit that checks split integrity and comparator fairness invariants before thesis claims. |

Plain-language explanation of comparative significance for your professor:
- "This is the statistical fairness check: SDE and ML are scored under the exact same matching rules, then we test whether the precision difference is real or just noise."

## 9) Recommended Citation Anchors in Thesis Narrative

- Split integrity and temporal leakage avoidance: Roberts et al. (2017), Brookshire et al. (2024).
- Imbalance-aware precision-first evaluation: Elkan (2001), Saito and Rehmsmeier (2015), Davis and Goadrich (2006).
- Comparative test rigor and uncertainty framing: Dietterich (1998), Walters (2022).
- Calibration reporting and probability validity framing: Brier (1950), Platt (1999), Guo et al. (2017), Kull et al. (2017).

