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
[moa_temporal_permutation_heatmap.pdf](moa_temporal_permutation_heatmap.pdf)

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

## 9) Citation Anchors in Thesis Narrative

- Split integrity and temporal leakage avoidance: Roberts et al. (2017), Brookshire et al. (2024).
- Imbalance-aware precision-first evaluation: Elkan (2001), Saito and Rehmsmeier (2015), Davis and Goadrich (2006).
- Comparative test rigor and uncertainty framing: Dietterich (1998), Walters (2022).
- Calibration reporting and probability validity framing: Brier (1950), Platt (1999), Guo et al. (2017), Kull et al. (2017).

## 10) `_source_year` Removal Ablation (Batch + Streaming, Added 2026-05-01)

This section adds finalized no-`_source_year` results from `data_lake/reports/no_source_year` and does **not** replace the full-feature baseline sections above.

### 10.1) What Was Re-run

- Batch ML pretrain (`expanding_race`) with `--drop-source-year-feature`, then full evaluator stack.
- Batch ML racewise (`expanding_race_sequential`) with `--drop-source-year-feature`, then comparator + threshold frontier.
- Streaming MOA with exported no-`_source_year` ARFF, prequential run, comparator mapping, surrogate SHAP proxy, and temporal permutation.

### 10.2) Decision-Level Comparator Results (No-`_source_year`)

| Approach | Actionable | Scored | TP | FP | Precision | Coverage |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SDE | 6,323 | 1,020 | 749 | 271 | 0.734314 | 0.161316 |
| ML-pretrain-base | 495 | 311 | 300 | 11 | 0.964630 | 0.628283 |
| ML-pretrain-extended | 22,892 | 3,387 | 2,904 | 483 | 0.857396 | 0.147956 |
| ML-racewise-base | 1,995 | 383 | 348 | 35 | 0.908616 | 0.191980 |
| ML-racewise-extended | 26,693 | 3,589 | 3,030 | 559 | 0.844246 | 0.134455 |
| MOA | 7,119 | 1,687 | 1,482 | 205 | 0.878483 | 0.236971 |

Delta vs full-feature baseline:

| Method | Precision Delta | Coverage Delta | Scored Delta | TP Delta | FP Delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| ML-pretrain-base | +0.021773 | +0.042653 | -284 | -261 | -23 |
| ML-pretrain-extended | +0.013529 | +0.036112 | -347 | -247 | -100 |
| ML-racewise-base | -0.005626 | -0.080103 | -270 | -249 | -21 |
| ML-racewise-extended | +0.010104 | +0.028334 | -324 | -234 | -90 |
| MOA | -0.025713 | +0.015369 | +257 | +189 | +68 |

### 10.3) OOF Discrimination (No-`_source_year`)

Overall calibrated PR-AUC deltas:
- `ml_pretrain`: `0.394339 -> 0.417826` (`+0.023487`)
- `ml_racewise`: `0.371696 -> 0.392840` (`+0.021144`)

No-`_source_year` PR metrics are in:
- `data_lake/reports/no_source_year/pr_metrics_2022_2025_no_source_year.csv`
- `data_lake/reports/no_source_year/pr_operating_points_2022_2025_no_source_year.csv`

No-`_source_year` PR figures:
- `pr_curves_overall_2022_2025_no_source_year.(pdf|png)`
- `pr_curves_by_year_2022_2025_no_source_year.(pdf|png)`
- `pr_curves_panel_2022_2025_no_source_year.(pdf|png)`

### 10.4) Threshold and Policy Effects

Selected no-`_source_year` best-threshold points:
- Pretrain extended: threshold `0.05`, precision `0.857396`, scored `3,387`
- Racewise extended: threshold `0.05`, precision `0.844246`, scored `3,589`

Calibration/policy (pretrain no-`_source_year`):
- constrained precision `0.818182`
- constrained recall `0.052427`
- reachability ratio `0.922604`
- fallback rate `0.013514`

### 10.5) Gates and Decision

Compared to full-feature run:
- Integrated gate `H1`: `NO_GO -> GO`
- Reachability `D2`: `0.899672 -> 0.922604` (`FAIL -> PASS`)
- Split-integrity `J1`: remains `FAIL` (same fold-assumption limitation)
- Comparator invariance `J2`: remains `PASS`

### 10.6) Streaming MOA Effects (No-`_source_year`)

Decision-level comparator:
- precision `0.904196 -> 0.878483`
- coverage `0.221602 -> 0.236971`

Prequential diagnostics:
- accuracy `89.447058% -> 89.416446%` (`-0.030612 pp`)
- kappa `13.696910% -> 16.993183%` (`+3.296273 pp`)

MOA explainability consistency:
- selected surrogate remains `rf_300_d12` in both SHAP proxy and temporal permutation summaries
- fidelity F1 remains aligned at `0.232266`

### 10.7) Artifact Paths (No-`_source_year`)

- Batch pretrain: `ml_oof_winner_2022_2025_no_source_year.csv`, `ml_ablation_phase31c_2022_2025_no_source_year.csv`
- Batch racewise: `ml_oof_winner_2022_2025_racewise_no_source_year.csv`, `ml_ablation_phase31c_2022_2025_racewise_no_source_year.csv`
- No-source comparators: `ml_comparator_2022_2025_no_source_year.csv`, `ml_comparator_best_threshold_2022_2025_no_source_year.csv`, `ml_comparator_2022_2025_racewise_no_source_year.csv`, `ml_comparator_best_threshold_2022_2025_racewise_no_source_year.csv`
- MOA no-source core: `moa_arf_summary_2022_2025_no_source_year.csv`, `moa_comparator_2022_2025_no_source_year.csv`
- MOA no-source explainability: `moa_shap_proxy_summary.csv`, `moa_temporal_permutation_summary.csv`, `moa_surrogate_pr_metrics.csv`, `moa_surrogate_pr_curve.png` (inside `data_lake/reports/no_source_year/`)

### 10.8) Comprehensive Delta Ledger (Full-Feature -> No-`_source_year`)

| Domain | Statistic | Full-Feature | No-`_source_year` | Delta |
| --- | --- | ---: | ---: | ---: |
| Comparator (ML-pretrain-base) | Precision | 0.942857 | 0.964630 | +0.021773 |
| Comparator (ML-pretrain-base) | Coverage | 0.585630 | 0.628283 | +0.042653 |
| Comparator (ML-pretrain-base) | Scored | 595 | 311 | -284 |
| Comparator (ML-pretrain-base) | TP | 561 | 300 | -261 |
| Comparator (ML-pretrain-base) | FP | 34 | 11 | -23 |
| Comparator (ML-pretrain-extended) | Precision | 0.843867 | 0.857396 | +0.013529 |
| Comparator (ML-pretrain-extended) | Coverage | 0.111843 | 0.147956 | +0.036112 |
| Comparator (ML-racewise-base) | Precision | 0.914242 | 0.908616 | -0.005626 |
| Comparator (ML-racewise-base) | Coverage | 0.272083 | 0.191980 | -0.080103 |
| Comparator (ML-racewise-extended) | Precision | 0.834143 | 0.844246 | +0.010104 |
| Comparator (ML-racewise-extended) | Coverage | 0.106121 | 0.134455 | +0.028334 |
| Comparator (MOA) | Precision | 0.904196 | 0.878483 | -0.025713 |
| Comparator (MOA) | Coverage | 0.221602 | 0.236971 | +0.015369 |
| Comparator (MOA) | Scored | 1430 | 1687 | +257 |
| Significance | ML minus SDE precision delta (`B1`) | 0.208543 | 0.230316 | +0.021773 |
| Threshold policy | Reference precision at `t=0.10` (`C1`) | 0.877658 | 0.893685 | +0.016027 |
| Threshold policy | No-match dominance (`C2`) | 0.968374 | 0.966372 | -0.002002 |
| Calibration policy | Constrained precision (`D1`) | 0.803150 | 0.818182 | +0.015032 |
| Calibration policy | Reachability ratio (`D2`) | 0.899672 | 0.922604 | +0.022932 |
| Calibration policy | Fallback rate (`D3`) | 0.000000 | 0.013514 | +0.013514 |
| Calibration policy | Constrained recall | 0.105631 | 0.052427 | -0.053204 |
| Calibration policy | Constrained F1 | 0.186706 | 0.098540 | -0.088166 |
| Calibration quality | Raw ECE | 0.261434 | 0.216759 | -0.044675 |
| Calibration quality | Calibrated ECE | 0.006690 | 0.020406 | +0.013716 |
| Runtime | p95 latency (`G1`, ms) | 10.268393 | 10.002881 | -0.265512 |
| PR (OOF, pretrain, calibrated) | AP | 0.394339 | 0.417826 | +0.023487 |
| PR (OOF, pretrain, raw) | AP | 0.402136 | 0.427274 | +0.025138 |
| PR (OOF, racewise, calibrated) | AP | 0.371696 | 0.392840 | +0.021144 |
| PR (OOF, racewise, raw) | AP | 0.381090 | 0.405532 | +0.024442 |
| MOA prequential | Accuracy (%) | 89.447058 | 89.416446 | -0.030612 |
| MOA prequential | Kappa (%) | 13.696910 | 16.993183 | +3.296273 |
| Integrated gate | Decision (`H1`) | NO_GO | GO | status improvement |
| Split-integrity gate | Status (`J1`) | FAIL | FAIL | unchanged |

### 10.9) SHAP Analysis (Batch and MOA Surrogate)

Batch SHAP comparison (top global drivers):

| Rank | Full-Feature Batch SHAP | Mean abs SHAP | No-`_source_year` Batch SHAP | Mean abs SHAP |
| ---: | --- | ---: | --- | ---: |
| 1 | `_source_year` | 0.419389 | `tire_life_ratio` | 0.381615 |
| 2 | `tire_life_ratio` | 0.406036 | `humidity` | 0.299820 |
| 3 | `trackTemp` | 0.251564 | `trackTemp` | 0.273737 |
| 4 | `humidity` | 0.251033 | `airTemp` | 0.238526 |
| 5 | `lapTime` | 0.245820 | `lapTime` | 0.220401 |

MOA surrogate SHAP comparison (top global drivers):

| Rank | Full-Feature MOA SHAP Proxy | Mean abs SHAP | No-`_source_year` MOA SHAP Proxy | Mean abs SHAP |
| ---: | --- | ---: | --- | ---: |
| 1 | `_source_year` | 0.051279 | `gap_to_physical_car` | 0.028079 |
| 2 | `speedTrap` | 0.034801 | `gapBehind` | 0.024028 |
| 3 | `gapBehind` | 0.032626 | `pace_drop_ratio` | 0.023950 |
| 4 | `gapAhead` | 0.029397 | `humidity` | 0.023296 |
| 5 | `trackTemp` | 0.021724 | `gapAhead` | 0.022225 |

Consistency checks:
- Batch top-10 overlap (full vs no-source): 9/10 common features.
- MOA top-10 overlap (full vs no-source): 7/10 common features.
- MOA surrogate selected model remains `rf_300_d12` in both SHAP proxy and temporal permutation summaries for the no-source run.

No-`_source_year` SHAP artifacts:
- Batch: `data_lake/reports/no_source_year/shap_feature_importance.csv`, `shap_global_bar.png`, `shap_beeswarm.png`
- MOA surrogate: `data_lake/reports/no_source_year/moa_shap_proxy_feature_importance.csv`, `moa_shap_proxy_global_bar.png`, `moa_shap_proxy_beeswarm.png`

No-`_source_year` SHAP figures:

Batch no-`_source_year` SHAP:
![Batch no-source SHAP global bar](no_source_year/shap_global_bar.png)
![Batch no-source SHAP beeswarm](no_source_year/shap_beeswarm.png)

MOA no-`_source_year` surrogate SHAP:
![MOA no-source SHAP proxy global bar](no_source_year/moa_shap_proxy_global_bar.png)
![MOA no-source SHAP proxy beeswarm](no_source_year/moa_shap_proxy_beeswarm.png)

MOA surrogate fidelity PR comparison (holdout set, target = decoded MOA decision):

| Variant | AP | PR-AUC (Trapz) | Test Prevalence | Surrogate Fidelity F1 | Surrogate Fidelity Accuracy |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full-feature baseline | 0.213560 | 0.213025 | 0.052388 | 0.230091 | 0.776772 |
| No-`_source_year` | 0.176950 | 0.176536 | 0.057799 | 0.232266 | 0.751796 |
| Delta (no-source minus full) | -0.036610 | -0.036489 | +0.005411 | +0.002175 | -0.024976 |

MOA surrogate PR artifacts:
- Full-feature: `data_lake/reports/moa_surrogate_pr_metrics.csv`, `moa_surrogate_pr_curve.png`
- No-`_source_year`: `data_lake/reports/no_source_year/moa_surrogate_pr_metrics.csv`, `moa_surrogate_pr_curve.png`

MOA surrogate PR figures:
![MOA full-feature surrogate PR curve](moa_surrogate_pr_curve.png)
![MOA no-source surrogate PR curve](no_source_year/moa_surrogate_pr_curve.png)

### 10.10) Next Shared Feature-Drop Candidates (Batch + MOA Consistent)

Conservative first-pass removal candidates (low SHAP in both no-source Batch and no-source MOA surrogate):

| Feature | Batch mean abs SHAP | MOA surrogate mean abs SHAP | Note |
| --- | ---: | ---: | --- |
| `team_nan` | 0.000000 | 0.000000 | pure missing-category indicator |
| `team_RB` | 0.001794 | 0.000092 | very low in both models |
| `team_Kick Sauber` | 0.002563 | 0.000493 | very low in both models |
| `team_Racing Bulls` | 0.003343 | 0.000727 | very low in both models |
| `team_Ferrari` | 0.007254 | 0.000857 | low in both models |
| `rainfall` | 0.002709 | 0.000643 | near-zero global contribution |
| `hasGapAhead` | 0.004337 | 0.000391 | likely redundant with numeric gap feature |

Suggested ablation protocol:
- Drop this exact shared set in both Batch and MOA exports to keep cross-paradigm fairness.
- Keep `hasGapBehind` for now (low in Batch but relatively higher in MOA than the conservative set above).

## 11) Multi-Profile Feature-Drop Matrix (E1/E2/E3, Added 2026-05-01)

This section appends the four completed profile runs executed after the no-`_source_year` baseline:
- `E0`: no-`_source_year` baseline (reference)
- `E1`: `drop_medium_v1`
- `E2a`: `track_agnostic_v1` (medium drop + causal expanding-z features, raw absolute retained)
- `E2b`: `track_agnostic_v1_strict` (E2a + absolute `trackTemp/airTemp/humidity/speedTrap/lapTime` removed)
- `E3`: `drop_aggressive_v1_candidate`

### 11.1) Exact Dataset Transformations (Batch + MOA) Before Testing

Common preprocessing applied to all E0/E1/E2/E3 runs (unchanged pipeline contract):

| Stage | What is done |
| --- | --- |
| Key integrity | Dedup on `(race, driver, lapNumber)` with `keep=last` before feature derivation; same for pit-evals on `(race, driver, pitLapNumber)` |
| Structural null handling | `gapAhead/gapBehind` missing values filled with structural sentinel `999.0`; boundary flags preserved via `hasGapAhead/hasGapBehind` |
| Derived causal features | `pace_drop_ratio`, `tire_life_ratio`, `pace_trend` (`shift(2)`), `gapAhead_trend` (`shift(2)`), drop-zone context joins |
| Label generation | Binary target with fixed horizon `H=2`, first future pit in `[k+1, k+2]`, stored in `label_horizon_laps` |
| Encoding | Matrix build via one-hot on categorical columns, boolean cast to numeric |
| No-source setting | All runs in this section used `--drop-source-year-feature` (feature removed from training/export matrices, while year metadata remains for split/evaluation bookkeeping) |

Experiment-specific feature contracts:

| Experiment | Profile | Track-Agnostic Mode | Features removed from model matrix | Extra features added to dataset |
| --- | --- | --- | --- | --- |
| E0 | baseline no-source | `off` | `_source_year` | none |
| E1 | `drop_medium_v1` | `off` | `team`, `rainfall`, `hasGapAhead`, `hasGapBehind`, `_source_year` | none |
| E2a | `track_agnostic_v1` | `track_agnostic_v1` | `team`, `rainfall`, `hasGapAhead`, `hasGapBehind`, `_source_year` | `trackTemp_expanding_z`, `airTemp_expanding_z`, `humidity_expanding_z`, `speedTrap_expanding_z`, `lapTime_expanding_z` |
| E2b | `track_agnostic_v1` + strict excludes | `track_agnostic_v1` | E2a removals + `trackTemp`, `airTemp`, `humidity`, `speedTrap`, `lapTime` + `_source_year` | same five expanding-z features as E2a |
| E3 | `drop_aggressive_v1_candidate` | `off` | E1 removals + `pitLoss`, `pace_trend`, `has_drop_zone_data`, `_source_year` | none |

Track-agnostic z-score definition used in E2a/E2b (exactly what is computed):

For each race and each source feature `x ∈ {trackTemp, airTemp, humidity, speedTrap, lapTime}`, rows are ordered by race timeline `(lapNumber, driver)` and transformed causally:

`μ_(t-1) = (1 / n_(t-1)) * Σ_{i < t} x_i`  
`σ_(t-1) = sqrt( (1 / n_(t-1)) * Σ_{i < t}(x_i^2) - μ_(t-1)^2 )`  
`z_t = (x_t - μ_(t-1)) / σ_(t-1)`

Guard behavior used in code:
- if `n_(t-1) < 2`, or `σ_(t-1) <= 1e-9`, or value is missing/non-finite: `z_t = 0.0`
- variance is clipped to non-negative before square-root

What values can these z-scores take:
- mathematical range is unbounded real `(-∞, +∞)`
- in practice, most rows are usually within roughly `[-3, +3]`
- `z = 0` means "at prior race mean" (or guard fallback)
- positive values mean above prior race context (`+2` means two prior std dev above mean)
- negative values mean below prior race context (`-1.5` means one and a half prior std dev below mean)

E2a vs E2b meaning:
- E2a keeps both raw absolute values and relative z-scores (model can use both anchors)
- E2b removes raw absolute channels and forces decisions to rely on relative context features plus other remaining variables

MOA-side dataset/model treatment (all experiments, not Batch-only):

| Stage | What happens for MOA |
| --- | --- |
| Dataset export | `export_moa_dataset.py` reuses the same prepared matrix contract as Batch (`_prepare_matrix`), with the same profile/exclusion set and same track-agnostic mode |
| File format | Exported to CSV + ARFF with numeric one-hot columns and binary target `{0,1}` |
| Streaming learner | `run_moa_arf.py` runs MOA AdaptiveRandomForest prequentially on that ARFF (progressive test-then-train stream evaluation) |
| Decision comparator | `comparator_moa.py` decodes MOA predictions and maps them into the same fixed `H=2` comparator contract used by SDE/Batch |
| Explainability | MOA proxy SHAP and temporal permutation are re-run per experiment on each exported MOA dataset/prediction pair |

Why this matters for interpretation:
- any metric delta in E1/E2/E3 is attributable to feature contract changes, not comparator semantics changes
- Batch and MOA are not sharing the same model family, but they are evaluated under matched data-contract and comparator rules

### 11.2) Batch Comparator Results (Pretrain Protocol)

| Experiment | Precision | Scored | TP | FP | Coverage (Scored/Actionable) | Delta Precision vs E0 | Delta Scored vs E0 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| E0 no-source baseline | 0.964630 | 311 | 300 | 11 | 0.628283 | +0.000000 | 0 |
| E1 drop_medium_v1 | 0.950331 | 302 | 287 | 15 | 0.663736 | -0.014299 | -9 |
| E2a track_agnostic_v1 | 0.963731 | 193 | 186 | 7 | 0.616613 | -0.000900 | -118 |
| E2b track_agnostic_v1_strict | 0.931373 | 102 | 95 | 7 | 0.217484 | -0.033258 | -209 |
| E3 drop_aggressive_v1_candidate | 0.958730 | 315 | 302 | 13 | 0.636364 | -0.005900 | +4 |

### 11.3) Batch Comparator Results (Racewise Protocol)

| Experiment | Precision | Scored | TP | FP | Coverage (Scored/Actionable) | Delta Precision vs E0 | Delta Scored vs E0 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| E0 no-source baseline | 0.908616 | 383 | 348 | 35 | 0.191980 | +0.000000 | 0 |
| E1 drop_medium_v1 | 0.877500 | 400 | 351 | 49 | 0.127267 | -0.031116 | +17 |
| E2a track_agnostic_v1 | 0.878136 | 279 | 245 | 34 | 0.102461 | -0.030480 | -104 |
| E2b track_agnostic_v1_strict | 0.844595 | 148 | 125 | 23 | 0.126821 | -0.064022 | -235 |
| E3 drop_aggressive_v1_candidate | 0.890547 | 402 | 358 | 44 | 0.128189 | -0.018069 | +19 |

### 11.4) OOF PR-AUC (Raw Probabilities, Overall)

| Experiment | Pretrain AP | Delta vs E0 | Racewise AP | Delta vs E0 |
| --- | ---: | ---: | ---: | ---: |
| E0 no-source baseline | 0.427274 | +0.000000 | 0.405532 | +0.000000 |
| E1 drop_medium_v1 | 0.409920 | -0.017354 | 0.388948 | -0.016584 |
| E2a track_agnostic_v1 | 0.346195 | -0.081078 | 0.330135 | -0.075397 |
| E2b track_agnostic_v1_strict | 0.275320 | -0.151954 | 0.262040 | -0.143492 |
| E3 drop_aggressive_v1_candidate | 0.400412 | -0.026862 | 0.379261 | -0.026271 |

### 11.5) MOA Comparator + Prequential Effects

| Experiment | MOA Comparator Precision | Scored | TP | FP | Coverage | Delta Precision vs E0 | Prequential Accuracy (%) | Delta Accuracy (pp) | Prequential Kappa (%) | Delta Kappa (pp) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| E0 no-source baseline | 0.878483 | 1687 | 1482 | 205 | 0.236971 | +0.000000 | 89.416446 | +0.000000 | 16.993183 | +0.000000 |
| E1 drop_medium_v1 | 0.885215 | 1603 | 1419 | 184 | 0.240258 | +0.006733 | 89.873204 | +0.456757 | 16.870226 | -0.122956 |
| E2a track_agnostic_v1 | 0.863905 | 1521 | 1314 | 207 | 0.209822 | -0.014577 | 88.722852 | -0.693594 | 13.987479 | -3.005704 |
| E2b track_agnostic_v1_strict | 0.873069 | 1489 | 1300 | 189 | 0.224517 | -0.005413 | 89.664561 | +0.248115 | 14.778865 | -2.214318 |
| E3 drop_aggressive_v1_candidate | 0.894083 | 1369 | 1224 | 145 | 0.263726 | +0.015601 | 90.435490 | +1.019044 | 15.634306 | -1.358877 |

### 11.5b) MOA Surrogate Fidelity PR (Holdout Set)

| Experiment | Surrogate PR-AP | Delta AP vs E0 | PR-AUC (Trapz) | Fidelity F1 | Fidelity Accuracy | Selected Surrogate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| E0 no-source baseline | 0.176950 | +0.000000 | 0.176536 | 0.232266 | 0.751796 | rf_300_d12 |
| E1 drop_medium_v1 | 0.193507 | +0.016556 | 0.192940 | 0.244632 | 0.826228 | rf_300_d12 |
| E2a track_agnostic_v1 | 0.200472 | +0.023522 | 0.199742 | 0.239653 | 0.736028 | rf_300_d12 |
| E2b track_agnostic_v1_strict | 0.196760 | +0.019810 | 0.196097 | 0.236890 | 0.797369 | rf_300_d12 |
| E3 drop_aggressive_v1_candidate | 0.160022 | -0.016929 | 0.159151 | 0.187851 | 0.812165 | rf_300_d12 |

### 11.6) Gate Summary (Pretrain Evaluator Stack)

| Experiment | Integrated Gate (`H1`) | Reachability Gate (`D2`) | Reachability Value |
| --- | --- | --- | ---: |
| E0 no-source baseline | GO | PASS | 0.922604 |
| E1 drop_medium_v1 | GO | PASS | 0.935435 |
| E2a track_agnostic_v1 | NO_GO | FAIL | 0.871007 |
| E2b track_agnostic_v1_strict | NO_GO | FAIL | 0.742288 |
| E3 drop_aggressive_v1_candidate | GO | PASS | 0.903221 |

Interpretation:
- `E1` is the best conservative drop profile in this batch: gate-safe (`GO`) with only modest precision/AP degradation versus E0.
- `E2a/E2b` are currently too costly for this pipeline configuration (clear AP drop and `NO_GO` gate outcomes).
- `E3` preserves `GO`, improves MOA comparator precision, but still reduces Batch AP/precision vs E0.

### 11.7) Produced Graphs

E1 (`no_source_year_drop_medium_v1`):
![E1 PR panel](no_source_year_drop_medium_v1/pr_curves_panel_2022_2025_no_source_year_drop_medium_v1.png)
![E1 Batch SHAP global](no_source_year_drop_medium_v1/shap_global_bar.png)
![E1 MOA SHAP proxy global](no_source_year_drop_medium_v1/moa_shap_proxy_global_bar.png)
![E1 MOA SHAP proxy beeswarm](no_source_year_drop_medium_v1/moa_shap_proxy_beeswarm.png)
![E1 MOA surrogate PR curve](no_source_year_drop_medium_v1/moa_surrogate_pr_curve.png)

E2a (`no_source_year_drop_medium_track_agnostic_v1`):
![E2a PR panel](no_source_year_drop_medium_track_agnostic_v1/pr_curves_panel_2022_2025_no_source_year_drop_medium_track_agnostic_v1.png)
![E2a Batch SHAP global](no_source_year_drop_medium_track_agnostic_v1/shap_global_bar.png)
![E2a MOA SHAP proxy global](no_source_year_drop_medium_track_agnostic_v1/moa_shap_proxy_global_bar.png)
![E2a MOA SHAP proxy beeswarm](no_source_year_drop_medium_track_agnostic_v1/moa_shap_proxy_beeswarm.png)
![E2a MOA surrogate PR curve](no_source_year_drop_medium_track_agnostic_v1/moa_surrogate_pr_curve.png)

E2b (`no_source_year_track_agnostic_v1_strict`):
![E2b PR panel](no_source_year_track_agnostic_v1_strict/pr_curves_panel_2022_2025_no_source_year_track_agnostic_v1_strict.png)
![E2b Batch SHAP global](no_source_year_track_agnostic_v1_strict/shap_global_bar.png)
![E2b MOA SHAP proxy global](no_source_year_track_agnostic_v1_strict/moa_shap_proxy_global_bar.png)
![E2b MOA SHAP proxy beeswarm](no_source_year_track_agnostic_v1_strict/moa_shap_proxy_beeswarm.png)
![E2b MOA surrogate PR curve](no_source_year_track_agnostic_v1_strict/moa_surrogate_pr_curve.png)

E3 (`no_source_year_drop_aggressive_v1_candidate`):
![E3 PR panel](no_source_year_drop_aggressive_v1_candidate/pr_curves_panel_2022_2025_no_source_year_drop_aggressive_v1_candidate.png)
![E3 Batch SHAP global](no_source_year_drop_aggressive_v1_candidate/shap_global_bar.png)
![E3 MOA SHAP proxy global](no_source_year_drop_aggressive_v1_candidate/moa_shap_proxy_global_bar.png)
![E3 MOA SHAP proxy beeswarm](no_source_year_drop_aggressive_v1_candidate/moa_shap_proxy_beeswarm.png)
![E3 MOA surrogate PR curve](no_source_year_drop_aggressive_v1_candidate/moa_surrogate_pr_curve.png)

Complete artifact folders:
- `data_lake/reports/no_source_year_drop_medium_v1/`
- `data_lake/reports/no_source_year_drop_medium_track_agnostic_v1/`
- `data_lake/reports/no_source_year_track_agnostic_v1_strict/`
- `data_lake/reports/no_source_year_drop_aggressive_v1_candidate/`
- Baseline reference: `data_lake/reports/no_source_year/`

### 11.8) Possible Meaning of the Obtained Results (Given the Data Changes)

These are empirical interpretations from this dataset and protocol, not universal causal claims.

| Experiment | Observed pattern | Possible meaning |
| --- | --- | --- |
| E1 (`drop_medium_v1`) | Pretrain AP drops moderately (`-0.017`), precision drops slightly (`-0.014`), gate remains `GO` | Removed features likely contain low-to-medium marginal signal for ranking quality, but not enough to break deployment gates |
| E2a (`track_agnostic_v1`) | Large AP drop (pretrain `-0.081`, racewise `-0.075`), `H1=NO_GO`, `D2` fails (`0.871`) | Relative-only normalization features add instability/noise for this setup, or reduce useful absolute-scale information despite causal correctness |
| E2b (`track_agnostic_v1_strict`) | Strongest AP collapse (pretrain `-0.152`, racewise `-0.143`), `H1=NO_GO`, `D2` fails (`0.742`) | Removing absolute weather/speed/lap-time channels is too aggressive; current model still depends materially on absolute magnitudes |
| E3 (`drop_aggressive_v1_candidate`) | Moderate AP drop (pretrain `-0.027`, racewise `-0.026`), `H1=GO` | Even with more removals, the system can stay operationally valid, but discrimination quality degrades versus E0 |

Cross-paradigm note:
- MOA reacts differently from Batch in some profiles (for example E3 improves MOA comparator precision while Batch AP declines), which suggests feature utility is paradigm-dependent and should be selected by the primary thesis objective (Batch discrimination vs Streaming decision precision).
- Under current evidence, `E1` is the best conservative simplification, while E2-track-agnostic variants need redesign before they can be considered competitive.

## 12) Percentage-Based Race-Agnostic Matrix (P1-P5, Added 2026-05-01)

This section appends the completed percentage-based ablation family executed after E0/E1-E3:
- `P0`: `no_source_year` baseline (reference, already reported above)
- `P1`: `percent_conservative_v1`
- `P2`: `percent_team_v1`
- `P3`: `percent_race_team_v1`
- `P4`: `percent_race_team_v1_strict`
- `P5`: `percent_race_team_aggressive_v1`

### 12.1) Exact Dataset Transformations (Batch + MOA)

Common contract (same as previous sections):
- same dedup and target-generation pipeline (`H=2`, same comparator semantics)
- same one-hot encoding path
- `_source_year` dropped from model/export matrices in all P1-P5 runs

Percentage features introduced (causal, past-only):

1) `race_progress_pct`
- definition: `lapNumber / scheduled_race_laps(year, race)`
- scheduled laps come from versioned local metadata keyed by `(year, race_name)`
- clipping: `[0.0, 1.5]`
- fail-fast behavior: run stops if race/year mapping is missing

2) `lapTime_vs_driver_prev_mean_pct`
- definition: `lapTime_t / mean(lapTime_k)` for same `(race, driver)` and `k < t`
- denominator uses only previous laps
- fallback when denominator missing/non-finite: `1.0`
- clipping: `[0.5, 2.0]`

3) `lapTime_vs_team_prev_lapbucket_mean_pct` (team mode and above)
- for each prior lap bucket `l < t`, compute team mean lap time at lap `l`
- denominator is the mean of those prior team lap-bucket means
- no same-lap leakage (strict `l < t`)
- fallback `1.0`, clipping `[0.5, 2.0]`

4) `lapTime_vs_race_prev_lapbucket_mean_pct` (race+team mode)
- same logic as team version, but denominator built at race level
- strict prior lap buckets only (`l < t`)
- fallback `1.0`, clipping `[0.5, 2.0]`

Experiment-specific contracts:

| Experiment | Feature profile | Track mode | Excluded features | Added percentage features |
| --- | --- | --- | --- | --- |
| P1 | `percent_conservative_v1` | `track_percentage_v1` | `team`, `rainfall`, `hasGapAhead`, `hasGapBehind`, `_source_year` | `race_progress_pct`, `lapTime_vs_driver_prev_mean_pct` |
| P2 | `percent_team_v1` | `track_percentage_team_v1` | same as P1 | P1 + `lapTime_vs_team_prev_lapbucket_mean_pct` |
| P3 | `percent_race_team_v1` | `track_percentage_race_team_v1` | same as P1 | P2 + `lapTime_vs_race_prev_lapbucket_mean_pct` |
| P4 | `percent_race_team_v1_strict` | `track_percentage_race_team_v1` | P3 excludes + `lapTime` | same as P3 |
| P5 | `percent_race_team_aggressive_v1` | `track_percentage_race_team_v1` | P4 excludes + `pitLoss`, `pace_trend`, `has_drop_zone_data` | same as P3 |

### 12.2) Exact Data Used for This Section

Primary artifact roots:
- baseline reference: `data_lake/reports/no_source_year/`
- P1: `data_lake/reports/no_source_year_percent_conservative_v1/`
- P2: `data_lake/reports/no_source_year_percent_team_v1/`
- P3: `data_lake/reports/no_source_year_percent_race_team_v1/`
- P4: `data_lake/reports/no_source_year_percent_race_team_v1_strict/`
- P5: `data_lake/reports/no_source_year_percent_race_team_aggressive_v1/`

Metric source files per experiment:
- comparator precision/TP/FP/coverage: `ml_comparator_*.csv` (pretrain and racewise variants)
- PR-AUC: `pr_metrics_*.csv` (`score_type=raw_proba`, `year=overall`)
- prequential stream metrics: `moa_arf_summary_*.csv`
- integrated/gate statuses: `model_evaluation_*.csv`
- surrogate fidelity: `moa_shap_proxy_summary.csv`
- surrogate fidelity PR (holdout): `moa_surrogate_pr_metrics.csv`, `moa_surrogate_pr_curve.png`

MOA comparator note:
- in these P-runs, `comparator_moa.py` wrote MOA comparator CSVs to `data_lake/data_lake/reports/no_source_year_percent_*/...` because of relative output resolution.
- the MOA comparator metrics below are computed from those generated files.

### 12.3) Batch Comparator Results (Pretrain Protocol)

| Experiment | Precision | Scored | TP | FP | Coverage (Scored/Actionable) | Delta Precision vs E0 | Delta Scored vs E0 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| E0 no-source baseline | 0.964630 | 311 | 300 | 11 | 0.628283 | +0.000000 | 0 |
| P1 percent_conservative_v1 | 0.961290 | 155 | 149 | 6 | 0.720930 | -0.003340 | -156 |
| P2 percent_team_v1 | 0.949367 | 158 | 150 | 8 | 0.745283 | -0.015263 | -153 |
| P3 percent_race_team_v1 | 0.948276 | 174 | 165 | 9 | 0.649254 | -0.016354 | -137 |
| P4 percent_race_team_v1_strict | 0.956204 | 137 | 131 | 6 | 0.698980 | -0.008426 | -174 |
| P5 percent_race_team_aggressive_v1 | 0.957895 | 190 | 182 | 8 | 0.610932 | -0.006735 | -121 |

### 12.4) Batch Comparator Results (Racewise Protocol)

| Experiment | Precision | Scored | TP | FP | Coverage (Scored/Actionable) | Delta Precision vs E0 | Delta Scored vs E0 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| E0 no-source baseline | 0.908616 | 383 | 348 | 35 | 0.191980 | +0.000000 | 0 |
| P1 percent_conservative_v1 | 0.848361 | 244 | 207 | 37 | 0.128896 | -0.060256 | -139 |
| P2 percent_team_v1 | 0.846774 | 248 | 210 | 38 | 0.136714 | -0.061842 | -135 |
| P3 percent_race_team_v1 | 0.888372 | 215 | 191 | 24 | 0.372617 | -0.020244 | -168 |
| P4 percent_race_team_v1_strict | 0.859223 | 206 | 177 | 29 | 0.268930 | -0.049393 | -177 |
| P5 percent_race_team_aggressive_v1 | 0.910853 | 258 | 235 | 23 | 0.140984 | +0.002237 | -125 |

### 12.5) OOF PR-AUC (Raw Probabilities, Overall)

| Experiment | Pretrain AP | Delta vs E0 | Racewise AP | Delta vs E0 |
| --- | ---: | ---: | ---: | ---: |
| E0 no-source baseline | 0.427274 | +0.000000 | 0.405532 | +0.000000 |
| P1 percent_conservative_v1 | 0.422945 | -0.004329 | 0.401798 | -0.003734 |
| P2 percent_team_v1 | 0.414634 | -0.012639 | 0.392553 | -0.012980 |
| P3 percent_race_team_v1 | 0.406283 | -0.020991 | 0.385810 | -0.019722 |
| P4 percent_race_team_v1_strict | 0.399647 | -0.027626 | 0.379948 | -0.025584 |
| P5 percent_race_team_aggressive_v1 | 0.387775 | -0.039499 | 0.369630 | -0.035902 |

### 12.6) MOA Comparator + Prequential Effects

| Experiment | MOA Comparator Precision | Scored | TP | FP | Coverage | Delta Precision vs E0 | Prequential Accuracy (%) | Delta Accuracy (pp) | Prequential Kappa (%) | Delta Kappa (pp) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| E0 no-source baseline | 0.878483 | 1687 | 1482 | 205 | 0.236971 | +0.000000 | 89.416446 | +0.000000 | 16.993183 | +0.000000 |
| P1 percent_conservative_v1 | 0.907165 | 1605 | 1456 | 149 | 0.295145 | +0.028683 | 90.948637 | +1.532191 | 19.873719 | +2.880536 |
| P2 percent_team_v1 | 0.886481 | 1938 | 1718 | 220 | 0.230276 | +0.007998 | 88.968551 | -0.447896 | 19.223826 | +2.230643 |
| P3 percent_race_team_v1 | 0.884058 | 1587 | 1403 | 184 | 0.257630 | +0.005575 | 90.067345 | +0.650899 | 16.373926 | -0.619257 |
| P4 percent_race_team_v1_strict | 0.884772 | 1970 | 1743 | 227 | 0.236807 | +0.006289 | 89.258555 | -0.157891 | 19.716673 | +2.723491 |
| P5 percent_race_team_aggressive_v1 | 0.887359 | 1598 | 1418 | 180 | 0.237480 | +0.008877 | 89.637978 | +0.221531 | 16.206134 | -0.787048 |

### 12.7) Gate and Acceptance-Criteria Summary

Operational gates from `model_evaluation_2022_2025_*.csv`:

| Experiment | Integrated Gate (`H1`) | Feature Parity (`F1`) | Reachability Gate (`D2`) | Reachability Value |
| --- | --- | --- | --- | ---: |
| E0 no-source baseline | GO | PASS | PASS | 0.922604 |
| P1 percent_conservative_v1 | GO | PASS | PASS | 0.926426 |
| P2 percent_team_v1 | GO | PASS | PASS | 0.935708 |
| P3 percent_race_team_v1 | NO_GO | PASS | FAIL | 0.896533 |
| P4 percent_race_team_v1_strict | GO | PASS | PASS | 0.902948 |
| P5 percent_race_team_aggressive_v1 | NO_GO | PASS | FAIL | 0.874556 |

Acceptance criteria check versus E0 (strict thresholds from the plan):
- AP delta per protocol `>= -0.01`
- comparator precision delta per protocol `>= -0.01`
- scored coverage and actionable coverage each `>= 0.80 * E0`

Observed:
- `P1` is the only profile that satisfies AP deltas in both protocols.
- comparator precision deltas are weakest on racewise for `P1-P4` (only `P5` racewise precision is non-negative).
- all `P1-P5` fail the strict `>= 0.80 * E0` scored/actionable retention criterion in at least one protocol (often both), mainly due substantially lower scored/actionable counts under the new contracts.

### 12.8) SHAP / Surrogate Explainability Signals (Percent Profiles)

Batch SHAP (top repeated pattern across P1-P5):
- `race_progress_pct` is the dominant feature in all five profiles.
- `tire_life_ratio` remains consistently second.
- weather/speed (`humidity`, `trackTemp`, `airTemp`, `speedTrap`) remain relevant.
- ratio features (`lapTime_vs_driver_prev_mean_pct`, and in race/team modes also `lapTime_vs_race_prev_lapbucket_mean_pct`) enter top ranks but stay below `race_progress_pct` and `tire_life_ratio`.

MOA surrogate SHAP (secondary evidence):
- `race_progress_pct` is also consistently top-ranked in the surrogate explanation.
- fidelity remains low-to-moderate (`F1` roughly `0.239-0.306`), so MOA proxy SHAP remains interpretive support, not primary pruning evidence.

Surrogate fidelity summary:

| Experiment | Surrogate fidelity accuracy | Surrogate fidelity F1 | Rows used |
| --- | ---: | ---: | ---: |
| E0 no-source baseline | 0.751796 | 0.232266 | 123184 |
| P1 percent_conservative_v1 | 0.819994 | 0.238520 | 123437 |
| P2 percent_team_v1 | 0.765038 | 0.286137 | 123275 |
| P3 percent_race_team_v1 | 0.832563 | 0.238762 | 123386 |
| P4 percent_race_team_v1_strict | 0.801052 | 0.306433 | 123547 |
| P5 percent_race_team_aggressive_v1 | 0.808564 | 0.266128 | 123305 |

### 12.8b) MOA Surrogate Fidelity PR (Holdout Set)

| Experiment | Surrogate PR-AP | Delta AP vs E0 | PR-AUC (Trapz) | Fidelity F1 | Fidelity Accuracy | Selected Surrogate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| E0 no-source baseline | 0.176950 | +0.000000 | 0.176536 | 0.232266 | 0.751796 | rf_300_d12 |
| P1 percent_conservative_v1 | 0.194497 | +0.017547 | 0.194089 | 0.238520 | 0.819994 | rf_300_d12 |
| P2 percent_team_v1 | 0.244218 | +0.067268 | 0.243886 | 0.286137 | 0.765038 | rf_300_d12 |
| P3 percent_race_team_v1 | 0.195802 | +0.018852 | 0.195382 | 0.238762 | 0.832563 | rf_300_d12 |
| P4 percent_race_team_v1_strict | 0.250517 | +0.073567 | 0.250029 | 0.306433 | 0.801052 | rf_300_d12 |
| P5 percent_race_team_aggressive_v1 | 0.240924 | +0.063974 | 0.240399 | 0.266128 | 0.808564 | rf_300_d12 |

### 12.9) Produced Graphs

P1 (`no_source_year_percent_conservative_v1`):
![P1 PR panel](no_source_year_percent_conservative_v1/pr_curves_panel_2022_2025_no_source_year_percent_conservative_v1.png)
![P1 Batch SHAP global](no_source_year_percent_conservative_v1/shap_global_bar.png)
![P1 MOA SHAP proxy global](no_source_year_percent_conservative_v1/moa_shap_proxy_global_bar.png)
![P1 MOA SHAP proxy beeswarm](no_source_year_percent_conservative_v1/moa_shap_proxy_beeswarm.png)
![P1 MOA surrogate PR curve](no_source_year_percent_conservative_v1/moa_surrogate_pr_curve.png)

P2 (`no_source_year_percent_team_v1`):
![P2 PR panel](no_source_year_percent_team_v1/pr_curves_panel_2022_2025_no_source_year_percent_team_v1.png)
![P2 Batch SHAP global](no_source_year_percent_team_v1/shap_global_bar.png)
![P2 MOA SHAP proxy global](no_source_year_percent_team_v1/moa_shap_proxy_global_bar.png)
![P2 MOA SHAP proxy beeswarm](no_source_year_percent_team_v1/moa_shap_proxy_beeswarm.png)
![P2 MOA surrogate PR curve](no_source_year_percent_team_v1/moa_surrogate_pr_curve.png)

P3 (`no_source_year_percent_race_team_v1`):
![P3 PR panel](no_source_year_percent_race_team_v1/pr_curves_panel_2022_2025_no_source_year_percent_race_team_v1.png)
![P3 Batch SHAP global](no_source_year_percent_race_team_v1/shap_global_bar.png)
![P3 MOA SHAP proxy global](no_source_year_percent_race_team_v1/moa_shap_proxy_global_bar.png)
![P3 MOA SHAP proxy beeswarm](no_source_year_percent_race_team_v1/moa_shap_proxy_beeswarm.png)
![P3 MOA surrogate PR curve](no_source_year_percent_race_team_v1/moa_surrogate_pr_curve.png)

P4 (`no_source_year_percent_race_team_v1_strict`):
![P4 PR panel](no_source_year_percent_race_team_v1_strict/pr_curves_panel_2022_2025_no_source_year_percent_race_team_v1_strict.png)
![P4 Batch SHAP global](no_source_year_percent_race_team_v1_strict/shap_global_bar.png)
![P4 MOA SHAP proxy global](no_source_year_percent_race_team_v1_strict/moa_shap_proxy_global_bar.png)
![P4 MOA SHAP proxy beeswarm](no_source_year_percent_race_team_v1_strict/moa_shap_proxy_beeswarm.png)
![P4 MOA surrogate PR curve](no_source_year_percent_race_team_v1_strict/moa_surrogate_pr_curve.png)

P5 (`no_source_year_percent_race_team_aggressive_v1`):
![P5 PR panel](no_source_year_percent_race_team_aggressive_v1/pr_curves_panel_2022_2025_no_source_year_percent_race_team_aggressive_v1.png)
![P5 Batch SHAP global](no_source_year_percent_race_team_aggressive_v1/shap_global_bar.png)
![P5 MOA SHAP proxy global](no_source_year_percent_race_team_aggressive_v1/moa_shap_proxy_global_bar.png)
![P5 MOA SHAP proxy beeswarm](no_source_year_percent_race_team_aggressive_v1/moa_shap_proxy_beeswarm.png)
![P5 MOA surrogate PR curve](no_source_year_percent_race_team_aggressive_v1/moa_surrogate_pr_curve.png)

### 12.10) Interpretation

Main empirical takeaways from this percentage-based cluster:
- `P1` is the most balanced percentage profile: very small AP degradation vs E0, strong MOA gains, and `H1=GO`.
- `P2` keeps gates green but exceeds the `-0.01` AP tolerance in both protocols.
- `P3` and `P5` push farther on relative-context engineering but fail `D2` reachability and produce `H1=NO_GO`.
- `P4` (strict lap-time removal) is better than expected operationally (`H1=GO`) but still materially below E0 on AP.

Cross-paradigm note:
- Batch discrimination and MOA decision/prequential behavior diverge in this cluster (for example, MOA precision/accuracy often improves while Batch AP declines), so model-selection should keep Batch PR evidence primary for the thesis objective and treat MOA surrogate evidence as secondary.

<!-- BEGIN AUTO_SECTION_13 -->
## 13) Advanced Statistical Diagnostics

This section is auto-generated from artifact-only scripts (no retraining).
It summarizes calibration reliability, PR-Gain, decision-curve utility, AP-delta uncertainty, and per-race temporal drift across all completed families.

### 13.1 AP Delta Uncertainty (Race-Cluster Bootstrap)

| Run | Reference | Protocol | Score | AP Delta | 95% CI Low | 95% CI High | Point AP (Run) | Point AP (Ref) |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| E0 no_source_year | Full-feature baseline | ml_pretrain | calibrated_proba | 0.023487 | 0.011870 | 0.035261 | 0.417826 | 0.394339 |
| E0 no_source_year | Full-feature baseline | ml_pretrain | raw_proba | 0.025138 | 0.015150 | 0.036103 | 0.427274 | 0.402136 |
| E0 no_source_year | Full-feature baseline | ml_racewise | calibrated_proba | 0.021144 | 0.009889 | 0.032327 | 0.392840 | 0.371696 |
| E0 no_source_year | Full-feature baseline | ml_racewise | raw_proba | 0.024442 | 0.014959 | 0.035015 | 0.405532 | 0.381090 |
| E1 drop_medium_v1 | E0 no_source_year | ml_pretrain | calibrated_proba | -0.016548 | -0.021388 | -0.011145 | 0.401278 | 0.417826 |
| E1 drop_medium_v1 | E0 no_source_year | ml_pretrain | raw_proba | -0.017354 | -0.021995 | -0.012218 | 0.409920 | 0.427274 |
| E1 drop_medium_v1 | E0 no_source_year | ml_racewise | calibrated_proba | -0.016090 | -0.021046 | -0.010868 | 0.376750 | 0.392840 |
| E1 drop_medium_v1 | E0 no_source_year | ml_racewise | raw_proba | -0.016584 | -0.020850 | -0.011500 | 0.388948 | 0.405532 |
| E2a drop_medium_track_agnostic_v1 | E0 no_source_year | ml_pretrain | calibrated_proba | -0.081364 | -0.098549 | -0.062973 | 0.336462 | 0.417826 |
| E2a drop_medium_track_agnostic_v1 | E0 no_source_year | ml_pretrain | raw_proba | -0.081078 | -0.099033 | -0.063037 | 0.346195 | 0.427274 |
| E2a drop_medium_track_agnostic_v1 | E0 no_source_year | ml_racewise | calibrated_proba | -0.075699 | -0.092716 | -0.056341 | 0.317141 | 0.392840 |
| E2a drop_medium_track_agnostic_v1 | E0 no_source_year | ml_racewise | raw_proba | -0.075397 | -0.091945 | -0.058049 | 0.330135 | 0.405532 |
| E2b track_agnostic_v1_strict | E0 no_source_year | ml_pretrain | calibrated_proba | -0.153492 | -0.184503 | -0.120705 | 0.264334 | 0.417826 |
| E2b track_agnostic_v1_strict | E0 no_source_year | ml_pretrain | raw_proba | -0.151954 | -0.179714 | -0.123825 | 0.275320 | 0.427274 |
| E2b track_agnostic_v1_strict | E0 no_source_year | ml_racewise | calibrated_proba | -0.144657 | -0.174538 | -0.112032 | 0.248183 | 0.392840 |
| E2b track_agnostic_v1_strict | E0 no_source_year | ml_racewise | raw_proba | -0.143492 | -0.169251 | -0.116068 | 0.262040 | 0.405532 |
| E3 drop_aggressive_v1_candidate | E0 no_source_year | ml_pretrain | calibrated_proba | -0.026880 | -0.034475 | -0.018758 | 0.390945 | 0.417826 |
| E3 drop_aggressive_v1_candidate | E0 no_source_year | ml_pretrain | raw_proba | -0.026862 | -0.033682 | -0.018868 | 0.400412 | 0.427274 |
| E3 drop_aggressive_v1_candidate | E0 no_source_year | ml_racewise | calibrated_proba | -0.035477 | -0.047393 | -0.022169 | 0.357363 | 0.392840 |
| E3 drop_aggressive_v1_candidate | E0 no_source_year | ml_racewise | raw_proba | -0.026271 | -0.032844 | -0.018165 | 0.379261 | 0.405532 |
| P1 percent_conservative_v1 | E0 no_source_year | ml_pretrain | calibrated_proba | -0.004217 | -0.021540 | 0.012997 | 0.413608 | 0.417826 |
| P1 percent_conservative_v1 | E0 no_source_year | ml_pretrain | raw_proba | -0.004329 | -0.022231 | 0.013214 | 0.422945 | 0.427274 |
| P1 percent_conservative_v1 | E0 no_source_year | ml_racewise | calibrated_proba | -0.001058 | -0.016761 | 0.015579 | 0.391782 | 0.392840 |
| P1 percent_conservative_v1 | E0 no_source_year | ml_racewise | raw_proba | -0.003734 | -0.020829 | 0.012511 | 0.401798 | 0.405532 |
| P2 percent_team_v1 | E0 no_source_year | ml_pretrain | calibrated_proba | -0.012868 | -0.030239 | 0.003736 | 0.404958 | 0.417826 |
| P2 percent_team_v1 | E0 no_source_year | ml_pretrain | raw_proba | -0.012639 | -0.031045 | 0.004829 | 0.414634 | 0.427274 |
| P2 percent_team_v1 | E0 no_source_year | ml_racewise | calibrated_proba | -0.012296 | -0.029598 | 0.004865 | 0.380544 | 0.392840 |
| P2 percent_team_v1 | E0 no_source_year | ml_racewise | raw_proba | -0.012980 | -0.030703 | 0.002964 | 0.392553 | 0.405532 |
| P3 percent_race_team_v1 | E0 no_source_year | ml_pretrain | calibrated_proba | -0.020615 | -0.038593 | -0.002145 | 0.397211 | 0.417826 |
| P3 percent_race_team_v1 | E0 no_source_year | ml_pretrain | raw_proba | -0.020991 | -0.041044 | -0.002665 | 0.406283 | 0.427274 |
| P3 percent_race_team_v1 | E0 no_source_year | ml_racewise | calibrated_proba | -0.015565 | -0.032693 | 0.002274 | 0.377275 | 0.392840 |
| P3 percent_race_team_v1 | E0 no_source_year | ml_racewise | raw_proba | -0.019722 | -0.038298 | -0.002558 | 0.385810 | 0.405532 |
| P4 percent_race_team_v1_strict | E0 no_source_year | ml_pretrain | calibrated_proba | -0.027039 | -0.047134 | -0.007415 | 0.390787 | 0.417826 |
| P4 percent_race_team_v1_strict | E0 no_source_year | ml_pretrain | raw_proba | -0.027626 | -0.048475 | -0.007362 | 0.399647 | 0.427274 |
| P4 percent_race_team_v1_strict | E0 no_source_year | ml_racewise | calibrated_proba | -0.023366 | -0.041975 | -0.004476 | 0.369474 | 0.392840 |
| P4 percent_race_team_v1_strict | E0 no_source_year | ml_racewise | raw_proba | -0.025584 | -0.045366 | -0.007089 | 0.379948 | 0.405532 |
| P5 percent_race_team_aggressive_v1 | E0 no_source_year | ml_pretrain | calibrated_proba | -0.042883 | -0.064656 | -0.020647 | 0.374943 | 0.417826 |
| P5 percent_race_team_aggressive_v1 | E0 no_source_year | ml_pretrain | raw_proba | -0.039499 | -0.059932 | -0.018436 | 0.387775 | 0.427274 |
| P5 percent_race_team_aggressive_v1 | E0 no_source_year | ml_racewise | calibrated_proba | -0.037132 | -0.057543 | -0.015801 | 0.355707 | 0.392840 |
| P5 percent_race_team_aggressive_v1 | E0 no_source_year | ml_racewise | raw_proba | -0.035902 | -0.055127 | -0.017096 | 0.369630 | 0.405532 |

- AP-delta forest plot: `ap_delta_forestplot.png`
- AP-delta forest plot: `ap_delta_forestplot.pdf`

### 13.2 Per-Run Advanced Metric Snapshot

| Run | Pretrain Calibrated ECE | Racewise Calibrated ECE | MOA Surrogate ECE | Mean Precision (SDE/Batch/MOA) | Mean Coverage (SDE/Batch/MOA) |
| --- | ---: | ---: | ---: | --- | --- |
| Full-feature baseline | 0.006578 | 0.005830 | 0.290879 | 0.7333/0.9607/0.8851 | 0.1625/0.6935/0.2337 |
| E0 no_source_year | 0.020201 | 0.016241 | 0.304361 | 0.7333/0.9722/0.8475 | 0.1625/0.7273/0.2438 |
| E1 drop_medium_v1 | 0.020080 | 0.016258 | 0.262638 | 0.7333/0.9464/0.8410 | 0.1625/0.7311/0.2401 |
| E2a drop_medium_track_agnostic_v1 | 0.016760 | 0.013203 | 0.276754 | 0.7333/0.9750/0.8397 | 0.1625/0.7129/0.2168 |
| E2b track_agnostic_v1_strict | 0.016760 | 0.012516 | 0.263975 | 0.7333/0.9389/0.8458 | 0.1625/0.6253/0.2248 |
| E3 drop_aggressive_v1_candidate | 0.019183 | 0.014918 | 0.263441 | 0.7333/0.9592/0.8863 | 0.1625/0.6887/0.3037 |
| P1 percent_conservative_v1 | 0.019790 | 0.015723 | 0.238494 | 0.7333/0.9693/0.8648 | 0.1625/0.7571/0.2975 |
| P2 percent_team_v1 | 0.019932 | 0.015914 | 0.254487 | 0.7333/0.9670/0.8777 | 0.1625/0.7892/0.2580 |
| P3 percent_race_team_v1 | 0.018865 | 0.015266 | 0.257405 | 0.7333/0.9487/0.8753 | 0.1625/0.7653/0.2624 |
| P4 percent_race_team_v1_strict | 0.018941 | 0.014900 | 0.244794 | 0.7333/0.9320/0.8603 | 0.1625/0.7230/0.2589 |
| P5 percent_race_team_aggressive_v1 | 0.018059 | 0.014299 | 0.228048 | 0.7333/0.9239/0.8581 | 0.1625/0.7461/0.2407 |

### 13.3 Artifact Links by Run

- Full-feature baseline (`.`):
  - `advanced_calibration_batch.png`
  - `advanced_calibration_moa_surrogate.png`
  - `advanced_pr_gain_batch.png`
  - `advanced_decision_curve_batch.png`
  - `advanced_decision_curve_moa_surrogate.png`
  - `advanced_temporal_drift_by_race.png`
- E0 no_source_year (`no_source_year`):
  - `no_source_year/advanced_calibration_batch.png`
  - `no_source_year/advanced_calibration_moa_surrogate.png`
  - `no_source_year/advanced_pr_gain_batch.png`
  - `no_source_year/advanced_decision_curve_batch.png`
  - `no_source_year/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year/advanced_temporal_drift_by_race.png`
- E1 drop_medium_v1 (`no_source_year_drop_medium_v1`):
  - `no_source_year_drop_medium_v1/advanced_calibration_batch.png`
  - `no_source_year_drop_medium_v1/advanced_calibration_moa_surrogate.png`
  - `no_source_year_drop_medium_v1/advanced_pr_gain_batch.png`
  - `no_source_year_drop_medium_v1/advanced_decision_curve_batch.png`
  - `no_source_year_drop_medium_v1/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_drop_medium_v1/advanced_temporal_drift_by_race.png`
- E2a drop_medium_track_agnostic_v1 (`no_source_year_drop_medium_track_agnostic_v1`):
  - `no_source_year_drop_medium_track_agnostic_v1/advanced_calibration_batch.png`
  - `no_source_year_drop_medium_track_agnostic_v1/advanced_calibration_moa_surrogate.png`
  - `no_source_year_drop_medium_track_agnostic_v1/advanced_pr_gain_batch.png`
  - `no_source_year_drop_medium_track_agnostic_v1/advanced_decision_curve_batch.png`
  - `no_source_year_drop_medium_track_agnostic_v1/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_drop_medium_track_agnostic_v1/advanced_temporal_drift_by_race.png`
- E2b track_agnostic_v1_strict (`no_source_year_track_agnostic_v1_strict`):
  - `no_source_year_track_agnostic_v1_strict/advanced_calibration_batch.png`
  - `no_source_year_track_agnostic_v1_strict/advanced_calibration_moa_surrogate.png`
  - `no_source_year_track_agnostic_v1_strict/advanced_pr_gain_batch.png`
  - `no_source_year_track_agnostic_v1_strict/advanced_decision_curve_batch.png`
  - `no_source_year_track_agnostic_v1_strict/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_track_agnostic_v1_strict/advanced_temporal_drift_by_race.png`
- E3 drop_aggressive_v1_candidate (`no_source_year_drop_aggressive_v1_candidate`):
  - `no_source_year_drop_aggressive_v1_candidate/advanced_calibration_batch.png`
  - `no_source_year_drop_aggressive_v1_candidate/advanced_calibration_moa_surrogate.png`
  - `no_source_year_drop_aggressive_v1_candidate/advanced_pr_gain_batch.png`
  - `no_source_year_drop_aggressive_v1_candidate/advanced_decision_curve_batch.png`
  - `no_source_year_drop_aggressive_v1_candidate/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_drop_aggressive_v1_candidate/advanced_temporal_drift_by_race.png`
- P1 percent_conservative_v1 (`no_source_year_percent_conservative_v1`):
  - `no_source_year_percent_conservative_v1/advanced_calibration_batch.png`
  - `no_source_year_percent_conservative_v1/advanced_calibration_moa_surrogate.png`
  - `no_source_year_percent_conservative_v1/advanced_pr_gain_batch.png`
  - `no_source_year_percent_conservative_v1/advanced_decision_curve_batch.png`
  - `no_source_year_percent_conservative_v1/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_percent_conservative_v1/advanced_temporal_drift_by_race.png`
- P2 percent_team_v1 (`no_source_year_percent_team_v1`):
  - `no_source_year_percent_team_v1/advanced_calibration_batch.png`
  - `no_source_year_percent_team_v1/advanced_calibration_moa_surrogate.png`
  - `no_source_year_percent_team_v1/advanced_pr_gain_batch.png`
  - `no_source_year_percent_team_v1/advanced_decision_curve_batch.png`
  - `no_source_year_percent_team_v1/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_percent_team_v1/advanced_temporal_drift_by_race.png`
- P3 percent_race_team_v1 (`no_source_year_percent_race_team_v1`):
  - `no_source_year_percent_race_team_v1/advanced_calibration_batch.png`
  - `no_source_year_percent_race_team_v1/advanced_calibration_moa_surrogate.png`
  - `no_source_year_percent_race_team_v1/advanced_pr_gain_batch.png`
  - `no_source_year_percent_race_team_v1/advanced_decision_curve_batch.png`
  - `no_source_year_percent_race_team_v1/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_percent_race_team_v1/advanced_temporal_drift_by_race.png`
- P4 percent_race_team_v1_strict (`no_source_year_percent_race_team_v1_strict`):
  - `no_source_year_percent_race_team_v1_strict/advanced_calibration_batch.png`
  - `no_source_year_percent_race_team_v1_strict/advanced_calibration_moa_surrogate.png`
  - `no_source_year_percent_race_team_v1_strict/advanced_pr_gain_batch.png`
  - `no_source_year_percent_race_team_v1_strict/advanced_decision_curve_batch.png`
  - `no_source_year_percent_race_team_v1_strict/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_percent_race_team_v1_strict/advanced_temporal_drift_by_race.png`
- P5 percent_race_team_aggressive_v1 (`no_source_year_percent_race_team_aggressive_v1`):
  - `no_source_year_percent_race_team_aggressive_v1/advanced_calibration_batch.png`
  - `no_source_year_percent_race_team_aggressive_v1/advanced_calibration_moa_surrogate.png`
  - `no_source_year_percent_race_team_aggressive_v1/advanced_pr_gain_batch.png`
  - `no_source_year_percent_race_team_aggressive_v1/advanced_decision_curve_batch.png`
  - `no_source_year_percent_race_team_aggressive_v1/advanced_decision_curve_moa_surrogate.png`
  - `no_source_year_percent_race_team_aggressive_v1/advanced_temporal_drift_by_race.png`

### 13.4 Key Figure Embeds (PNG)

The following figures are embedded directly for quick review.

#### Full-feature baseline
![Full-feature baseline - advanced_calibration_batch](advanced_calibration_batch.png)
![Full-feature baseline - advanced_pr_gain_batch](advanced_pr_gain_batch.png)
![Full-feature baseline - advanced_decision_curve_batch](advanced_decision_curve_batch.png)
![Full-feature baseline - advanced_temporal_drift_by_race](advanced_temporal_drift_by_race.png)

#### E0 no_source_year
![E0 no_source_year - advanced_calibration_batch](no_source_year/advanced_calibration_batch.png)
![E0 no_source_year - advanced_pr_gain_batch](no_source_year/advanced_pr_gain_batch.png)
![E0 no_source_year - advanced_decision_curve_batch](no_source_year/advanced_decision_curve_batch.png)
![E0 no_source_year - advanced_temporal_drift_by_race](no_source_year/advanced_temporal_drift_by_race.png)

#### E1 drop_medium_v1
![E1 drop_medium_v1 - advanced_calibration_batch](no_source_year_drop_medium_v1/advanced_calibration_batch.png)
![E1 drop_medium_v1 - advanced_pr_gain_batch](no_source_year_drop_medium_v1/advanced_pr_gain_batch.png)
![E1 drop_medium_v1 - advanced_decision_curve_batch](no_source_year_drop_medium_v1/advanced_decision_curve_batch.png)
![E1 drop_medium_v1 - advanced_temporal_drift_by_race](no_source_year_drop_medium_v1/advanced_temporal_drift_by_race.png)

#### E2a drop_medium_track_agnostic_v1
![E2a drop_medium_track_agnostic_v1 - advanced_calibration_batch](no_source_year_drop_medium_track_agnostic_v1/advanced_calibration_batch.png)
![E2a drop_medium_track_agnostic_v1 - advanced_pr_gain_batch](no_source_year_drop_medium_track_agnostic_v1/advanced_pr_gain_batch.png)
![E2a drop_medium_track_agnostic_v1 - advanced_decision_curve_batch](no_source_year_drop_medium_track_agnostic_v1/advanced_decision_curve_batch.png)
![E2a drop_medium_track_agnostic_v1 - advanced_temporal_drift_by_race](no_source_year_drop_medium_track_agnostic_v1/advanced_temporal_drift_by_race.png)

#### E2b track_agnostic_v1_strict
![E2b track_agnostic_v1_strict - advanced_calibration_batch](no_source_year_track_agnostic_v1_strict/advanced_calibration_batch.png)
![E2b track_agnostic_v1_strict - advanced_pr_gain_batch](no_source_year_track_agnostic_v1_strict/advanced_pr_gain_batch.png)
![E2b track_agnostic_v1_strict - advanced_decision_curve_batch](no_source_year_track_agnostic_v1_strict/advanced_decision_curve_batch.png)
![E2b track_agnostic_v1_strict - advanced_temporal_drift_by_race](no_source_year_track_agnostic_v1_strict/advanced_temporal_drift_by_race.png)

#### E3 drop_aggressive_v1_candidate
![E3 drop_aggressive_v1_candidate - advanced_calibration_batch](no_source_year_drop_aggressive_v1_candidate/advanced_calibration_batch.png)
![E3 drop_aggressive_v1_candidate - advanced_pr_gain_batch](no_source_year_drop_aggressive_v1_candidate/advanced_pr_gain_batch.png)
![E3 drop_aggressive_v1_candidate - advanced_decision_curve_batch](no_source_year_drop_aggressive_v1_candidate/advanced_decision_curve_batch.png)
![E3 drop_aggressive_v1_candidate - advanced_temporal_drift_by_race](no_source_year_drop_aggressive_v1_candidate/advanced_temporal_drift_by_race.png)

#### P1 percent_conservative_v1
![P1 percent_conservative_v1 - advanced_calibration_batch](no_source_year_percent_conservative_v1/advanced_calibration_batch.png)
![P1 percent_conservative_v1 - advanced_pr_gain_batch](no_source_year_percent_conservative_v1/advanced_pr_gain_batch.png)
![P1 percent_conservative_v1 - advanced_decision_curve_batch](no_source_year_percent_conservative_v1/advanced_decision_curve_batch.png)
![P1 percent_conservative_v1 - advanced_temporal_drift_by_race](no_source_year_percent_conservative_v1/advanced_temporal_drift_by_race.png)

#### P2 percent_team_v1
![P2 percent_team_v1 - advanced_calibration_batch](no_source_year_percent_team_v1/advanced_calibration_batch.png)
![P2 percent_team_v1 - advanced_pr_gain_batch](no_source_year_percent_team_v1/advanced_pr_gain_batch.png)
![P2 percent_team_v1 - advanced_decision_curve_batch](no_source_year_percent_team_v1/advanced_decision_curve_batch.png)
![P2 percent_team_v1 - advanced_temporal_drift_by_race](no_source_year_percent_team_v1/advanced_temporal_drift_by_race.png)

#### P3 percent_race_team_v1
![P3 percent_race_team_v1 - advanced_calibration_batch](no_source_year_percent_race_team_v1/advanced_calibration_batch.png)
![P3 percent_race_team_v1 - advanced_pr_gain_batch](no_source_year_percent_race_team_v1/advanced_pr_gain_batch.png)
![P3 percent_race_team_v1 - advanced_decision_curve_batch](no_source_year_percent_race_team_v1/advanced_decision_curve_batch.png)
![P3 percent_race_team_v1 - advanced_temporal_drift_by_race](no_source_year_percent_race_team_v1/advanced_temporal_drift_by_race.png)

#### P4 percent_race_team_v1_strict
![P4 percent_race_team_v1_strict - advanced_calibration_batch](no_source_year_percent_race_team_v1_strict/advanced_calibration_batch.png)
![P4 percent_race_team_v1_strict - advanced_pr_gain_batch](no_source_year_percent_race_team_v1_strict/advanced_pr_gain_batch.png)
![P4 percent_race_team_v1_strict - advanced_decision_curve_batch](no_source_year_percent_race_team_v1_strict/advanced_decision_curve_batch.png)
![P4 percent_race_team_v1_strict - advanced_temporal_drift_by_race](no_source_year_percent_race_team_v1_strict/advanced_temporal_drift_by_race.png)

#### P5 percent_race_team_aggressive_v1
![P5 percent_race_team_aggressive_v1 - advanced_calibration_batch](no_source_year_percent_race_team_aggressive_v1/advanced_calibration_batch.png)
![P5 percent_race_team_aggressive_v1 - advanced_pr_gain_batch](no_source_year_percent_race_team_aggressive_v1/advanced_pr_gain_batch.png)
![P5 percent_race_team_aggressive_v1 - advanced_decision_curve_batch](no_source_year_percent_race_team_aggressive_v1/advanced_decision_curve_batch.png)
![P5 percent_race_team_aggressive_v1 - advanced_temporal_drift_by_race](no_source_year_percent_race_team_aggressive_v1/advanced_temporal_drift_by_race.png)


<!-- END AUTO_SECTION_13 -->

<!-- BEGIN AUTO_SECTION_14 -->
## 14) Cross-Paradigm Error Analysis

This section is auto-generated from decision-level comparator rows (`H=2`) with SDE/Batch/MOA alignment on `(race, driver, suggestion_lap)`.

### 14.1 Run-Level Error Summary

| Run | Model | Actionable | Scored | TP | FP | Precision | FP Rate (Scored) | Excluded NO_MATCH |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full-feature baseline | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| Full-feature baseline | Batch | 1016 | 595 | 561 | 34 | 0.942857 | 0.057143 | 403 |
| Full-feature baseline | MOA | 6453 | 1430 | 1293 | 137 | 0.904196 | 0.095804 | 4835 |
| E0 no_source_year | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| E0 no_source_year | Batch | 495 | 311 | 300 | 11 | 0.964630 | 0.035370 | 173 |
| E0 no_source_year | MOA | 7119 | 1687 | 1482 | 205 | 0.878483 | 0.121517 | 5221 |
| E1 drop_medium_v1 | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| E1 drop_medium_v1 | Batch | 455 | 302 | 287 | 15 | 0.950331 | 0.049669 | 141 |
| E1 drop_medium_v1 | MOA | 6672 | 1603 | 1419 | 184 | 0.885215 | 0.114785 | 4867 |
| E2a drop_medium_track_agnostic_v1 | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| E2a drop_medium_track_agnostic_v1 | Batch | 313 | 193 | 186 | 7 | 0.963731 | 0.036269 | 108 |
| E2a drop_medium_track_agnostic_v1 | MOA | 7249 | 1521 | 1314 | 207 | 0.863905 | 0.136095 | 5519 |
| E2b track_agnostic_v1_strict | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| E2b track_agnostic_v1_strict | Batch | 469 | 102 | 95 | 7 | 0.931373 | 0.068627 | 359 |
| E2b track_agnostic_v1_strict | MOA | 6632 | 1489 | 1300 | 189 | 0.873069 | 0.126931 | 4915 |
| E3 drop_aggressive_v1_candidate | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| E3 drop_aggressive_v1_candidate | Batch | 495 | 315 | 302 | 13 | 0.958730 | 0.041270 | 169 |
| E3 drop_aggressive_v1_candidate | MOA | 5191 | 1369 | 1224 | 145 | 0.894083 | 0.105917 | 3645 |
| P1 percent_conservative_v1 | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| P1 percent_conservative_v1 | Batch | 215 | 155 | 149 | 6 | 0.961290 | 0.038710 | 55 |
| P1 percent_conservative_v1 | MOA | 5438 | 1605 | 1456 | 149 | 0.907165 | 0.092835 | 3638 |
| P2 percent_team_v1 | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| P2 percent_team_v1 | Batch | 212 | 158 | 150 | 8 | 0.949367 | 0.050633 | 49 |
| P2 percent_team_v1 | MOA | 8416 | 1938 | 1718 | 220 | 0.886481 | 0.113519 | 6200 |
| P3 percent_race_team_v1 | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| P3 percent_race_team_v1 | Batch | 268 | 174 | 165 | 9 | 0.948276 | 0.051724 | 86 |
| P3 percent_race_team_v1 | MOA | 6160 | 1587 | 1403 | 184 | 0.884058 | 0.115942 | 4363 |
| P4 percent_race_team_v1_strict | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| P4 percent_race_team_v1_strict | Batch | 196 | 137 | 131 | 6 | 0.956204 | 0.043796 | 53 |
| P4 percent_race_team_v1_strict | MOA | 8319 | 1970 | 1743 | 227 | 0.884772 | 0.115228 | 6079 |
| P5 percent_race_team_aggressive_v1 | SDE | 6323 | 1020 | 749 | 271 | 0.734314 | 0.265686 | 4873 |
| P5 percent_race_team_aggressive_v1 | Batch | 311 | 190 | 182 | 8 | 0.957895 | 0.042105 | 116 |
| P5 percent_race_team_aggressive_v1 | MOA | 6729 | 1598 | 1418 | 180 | 0.887359 | 0.112641 | 4910 |

### 14.2 FP Consensus Overlap Highlights

| Run | SDE only | Batch only | MOA only | SDE+Batch only | SDE+MOA only | Batch+MOA only | All three |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full-feature baseline | 248 | 21 | 111 | 5 | 18 | 8 | 0 |
| E0 no_source_year | 251 | 9 | 183 | 0 | 20 | 2 | 0 |
| E1 drop_medium_v1 | 250 | 9 | 166 | 4 | 16 | 1 | 1 |
| E2a drop_medium_track_agnostic_v1 | 247 | 2 | 180 | 1 | 23 | 4 | 0 |
| E2b track_agnostic_v1_strict | 243 | 4 | 162 | 2 | 26 | 1 | 0 |
| E3 drop_aggressive_v1_candidate | 255 | 8 | 126 | 1 | 15 | 4 | 0 |
| P1 percent_conservative_v1 | 257 | 5 | 134 | 0 | 14 | 1 | 0 |
| P2 percent_team_v1 | 239 | 8 | 188 | 0 | 32 | 0 | 0 |
| P3 percent_race_team_v1 | 259 | 5 | 168 | 0 | 12 | 4 | 0 |
| P4 percent_race_team_v1_strict | 252 | 5 | 207 | 0 | 19 | 1 | 0 |
| P5 percent_race_team_aggressive_v1 | 250 | 5 | 160 | 2 | 19 | 1 | 0 |

### 14.3 Error-Taxonomy Artifacts

- Full-feature baseline (`.`):
  - `error_consensus_upset.png`
  - `error_near_miss_distribution.png`
  - `error_batch_fp_tp_feature_profiles.png`
  - `error_taxonomy_manual_template.csv`
  - `error_taxonomy_summary.csv`
  - `error_consensus_overlap_counts.csv`
  - `error_near_miss_distribution.csv`
- E0 no_source_year (`no_source_year`):
  - `no_source_year/error_consensus_upset.png`
  - `no_source_year/error_near_miss_distribution.png`
  - `no_source_year/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year/error_taxonomy_manual_template.csv`
  - `no_source_year/error_taxonomy_summary.csv`
  - `no_source_year/error_consensus_overlap_counts.csv`
  - `no_source_year/error_near_miss_distribution.csv`
- E1 drop_medium_v1 (`no_source_year_drop_medium_v1`):
  - `no_source_year_drop_medium_v1/error_consensus_upset.png`
  - `no_source_year_drop_medium_v1/error_near_miss_distribution.png`
  - `no_source_year_drop_medium_v1/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_drop_medium_v1/error_taxonomy_manual_template.csv`
  - `no_source_year_drop_medium_v1/error_taxonomy_summary.csv`
  - `no_source_year_drop_medium_v1/error_consensus_overlap_counts.csv`
  - `no_source_year_drop_medium_v1/error_near_miss_distribution.csv`
- E2a drop_medium_track_agnostic_v1 (`no_source_year_drop_medium_track_agnostic_v1`):
  - `no_source_year_drop_medium_track_agnostic_v1/error_consensus_upset.png`
  - `no_source_year_drop_medium_track_agnostic_v1/error_near_miss_distribution.png`
  - `no_source_year_drop_medium_track_agnostic_v1/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_drop_medium_track_agnostic_v1/error_taxonomy_manual_template.csv`
  - `no_source_year_drop_medium_track_agnostic_v1/error_taxonomy_summary.csv`
  - `no_source_year_drop_medium_track_agnostic_v1/error_consensus_overlap_counts.csv`
  - `no_source_year_drop_medium_track_agnostic_v1/error_near_miss_distribution.csv`
- E2b track_agnostic_v1_strict (`no_source_year_track_agnostic_v1_strict`):
  - `no_source_year_track_agnostic_v1_strict/error_consensus_upset.png`
  - `no_source_year_track_agnostic_v1_strict/error_near_miss_distribution.png`
  - `no_source_year_track_agnostic_v1_strict/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_track_agnostic_v1_strict/error_taxonomy_manual_template.csv`
  - `no_source_year_track_agnostic_v1_strict/error_taxonomy_summary.csv`
  - `no_source_year_track_agnostic_v1_strict/error_consensus_overlap_counts.csv`
  - `no_source_year_track_agnostic_v1_strict/error_near_miss_distribution.csv`
- E3 drop_aggressive_v1_candidate (`no_source_year_drop_aggressive_v1_candidate`):
  - `no_source_year_drop_aggressive_v1_candidate/error_consensus_upset.png`
  - `no_source_year_drop_aggressive_v1_candidate/error_near_miss_distribution.png`
  - `no_source_year_drop_aggressive_v1_candidate/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_drop_aggressive_v1_candidate/error_taxonomy_manual_template.csv`
  - `no_source_year_drop_aggressive_v1_candidate/error_taxonomy_summary.csv`
  - `no_source_year_drop_aggressive_v1_candidate/error_consensus_overlap_counts.csv`
  - `no_source_year_drop_aggressive_v1_candidate/error_near_miss_distribution.csv`
- P1 percent_conservative_v1 (`no_source_year_percent_conservative_v1`):
  - `no_source_year_percent_conservative_v1/error_consensus_upset.png`
  - `no_source_year_percent_conservative_v1/error_near_miss_distribution.png`
  - `no_source_year_percent_conservative_v1/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_percent_conservative_v1/error_taxonomy_manual_template.csv`
  - `no_source_year_percent_conservative_v1/error_taxonomy_summary.csv`
  - `no_source_year_percent_conservative_v1/error_consensus_overlap_counts.csv`
  - `no_source_year_percent_conservative_v1/error_near_miss_distribution.csv`
- P2 percent_team_v1 (`no_source_year_percent_team_v1`):
  - `no_source_year_percent_team_v1/error_consensus_upset.png`
  - `no_source_year_percent_team_v1/error_near_miss_distribution.png`
  - `no_source_year_percent_team_v1/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_percent_team_v1/error_taxonomy_manual_template.csv`
  - `no_source_year_percent_team_v1/error_taxonomy_summary.csv`
  - `no_source_year_percent_team_v1/error_consensus_overlap_counts.csv`
  - `no_source_year_percent_team_v1/error_near_miss_distribution.csv`
- P3 percent_race_team_v1 (`no_source_year_percent_race_team_v1`):
  - `no_source_year_percent_race_team_v1/error_consensus_upset.png`
  - `no_source_year_percent_race_team_v1/error_near_miss_distribution.png`
  - `no_source_year_percent_race_team_v1/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_percent_race_team_v1/error_taxonomy_manual_template.csv`
  - `no_source_year_percent_race_team_v1/error_taxonomy_summary.csv`
  - `no_source_year_percent_race_team_v1/error_consensus_overlap_counts.csv`
  - `no_source_year_percent_race_team_v1/error_near_miss_distribution.csv`
- P4 percent_race_team_v1_strict (`no_source_year_percent_race_team_v1_strict`):
  - `no_source_year_percent_race_team_v1_strict/error_consensus_upset.png`
  - `no_source_year_percent_race_team_v1_strict/error_near_miss_distribution.png`
  - `no_source_year_percent_race_team_v1_strict/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_percent_race_team_v1_strict/error_taxonomy_manual_template.csv`
  - `no_source_year_percent_race_team_v1_strict/error_taxonomy_summary.csv`
  - `no_source_year_percent_race_team_v1_strict/error_consensus_overlap_counts.csv`
  - `no_source_year_percent_race_team_v1_strict/error_near_miss_distribution.csv`
- P5 percent_race_team_aggressive_v1 (`no_source_year_percent_race_team_aggressive_v1`):
  - `no_source_year_percent_race_team_aggressive_v1/error_consensus_upset.png`
  - `no_source_year_percent_race_team_aggressive_v1/error_near_miss_distribution.png`
  - `no_source_year_percent_race_team_aggressive_v1/error_batch_fp_tp_feature_profiles.png`
  - `no_source_year_percent_race_team_aggressive_v1/error_taxonomy_manual_template.csv`
  - `no_source_year_percent_race_team_aggressive_v1/error_taxonomy_summary.csv`
  - `no_source_year_percent_race_team_aggressive_v1/error_consensus_overlap_counts.csv`
  - `no_source_year_percent_race_team_aggressive_v1/error_near_miss_distribution.csv`

### 14.4 Error Figure Embeds (PNG)

#### Full-feature baseline
![Full-feature baseline - error_consensus_upset](error_consensus_upset.png)
![Full-feature baseline - error_near_miss_distribution](error_near_miss_distribution.png)
![Full-feature baseline - error_batch_fp_tp_feature_profiles](error_batch_fp_tp_feature_profiles.png)

#### E0 no_source_year
![E0 no_source_year - error_consensus_upset](no_source_year/error_consensus_upset.png)
![E0 no_source_year - error_near_miss_distribution](no_source_year/error_near_miss_distribution.png)
![E0 no_source_year - error_batch_fp_tp_feature_profiles](no_source_year/error_batch_fp_tp_feature_profiles.png)

#### E1 drop_medium_v1
![E1 drop_medium_v1 - error_consensus_upset](no_source_year_drop_medium_v1/error_consensus_upset.png)
![E1 drop_medium_v1 - error_near_miss_distribution](no_source_year_drop_medium_v1/error_near_miss_distribution.png)
![E1 drop_medium_v1 - error_batch_fp_tp_feature_profiles](no_source_year_drop_medium_v1/error_batch_fp_tp_feature_profiles.png)

#### E2a drop_medium_track_agnostic_v1
![E2a drop_medium_track_agnostic_v1 - error_consensus_upset](no_source_year_drop_medium_track_agnostic_v1/error_consensus_upset.png)
![E2a drop_medium_track_agnostic_v1 - error_near_miss_distribution](no_source_year_drop_medium_track_agnostic_v1/error_near_miss_distribution.png)
![E2a drop_medium_track_agnostic_v1 - error_batch_fp_tp_feature_profiles](no_source_year_drop_medium_track_agnostic_v1/error_batch_fp_tp_feature_profiles.png)

#### E2b track_agnostic_v1_strict
![E2b track_agnostic_v1_strict - error_consensus_upset](no_source_year_track_agnostic_v1_strict/error_consensus_upset.png)
![E2b track_agnostic_v1_strict - error_near_miss_distribution](no_source_year_track_agnostic_v1_strict/error_near_miss_distribution.png)
![E2b track_agnostic_v1_strict - error_batch_fp_tp_feature_profiles](no_source_year_track_agnostic_v1_strict/error_batch_fp_tp_feature_profiles.png)

#### E3 drop_aggressive_v1_candidate
![E3 drop_aggressive_v1_candidate - error_consensus_upset](no_source_year_drop_aggressive_v1_candidate/error_consensus_upset.png)
![E3 drop_aggressive_v1_candidate - error_near_miss_distribution](no_source_year_drop_aggressive_v1_candidate/error_near_miss_distribution.png)
![E3 drop_aggressive_v1_candidate - error_batch_fp_tp_feature_profiles](no_source_year_drop_aggressive_v1_candidate/error_batch_fp_tp_feature_profiles.png)

#### P1 percent_conservative_v1
![P1 percent_conservative_v1 - error_consensus_upset](no_source_year_percent_conservative_v1/error_consensus_upset.png)
![P1 percent_conservative_v1 - error_near_miss_distribution](no_source_year_percent_conservative_v1/error_near_miss_distribution.png)
![P1 percent_conservative_v1 - error_batch_fp_tp_feature_profiles](no_source_year_percent_conservative_v1/error_batch_fp_tp_feature_profiles.png)

#### P2 percent_team_v1
![P2 percent_team_v1 - error_consensus_upset](no_source_year_percent_team_v1/error_consensus_upset.png)
![P2 percent_team_v1 - error_near_miss_distribution](no_source_year_percent_team_v1/error_near_miss_distribution.png)
![P2 percent_team_v1 - error_batch_fp_tp_feature_profiles](no_source_year_percent_team_v1/error_batch_fp_tp_feature_profiles.png)

#### P3 percent_race_team_v1
![P3 percent_race_team_v1 - error_consensus_upset](no_source_year_percent_race_team_v1/error_consensus_upset.png)
![P3 percent_race_team_v1 - error_near_miss_distribution](no_source_year_percent_race_team_v1/error_near_miss_distribution.png)
![P3 percent_race_team_v1 - error_batch_fp_tp_feature_profiles](no_source_year_percent_race_team_v1/error_batch_fp_tp_feature_profiles.png)

#### P4 percent_race_team_v1_strict
![P4 percent_race_team_v1_strict - error_consensus_upset](no_source_year_percent_race_team_v1_strict/error_consensus_upset.png)
![P4 percent_race_team_v1_strict - error_near_miss_distribution](no_source_year_percent_race_team_v1_strict/error_near_miss_distribution.png)
![P4 percent_race_team_v1_strict - error_batch_fp_tp_feature_profiles](no_source_year_percent_race_team_v1_strict/error_batch_fp_tp_feature_profiles.png)

#### P5 percent_race_team_aggressive_v1
![P5 percent_race_team_aggressive_v1 - error_consensus_upset](no_source_year_percent_race_team_aggressive_v1/error_consensus_upset.png)
![P5 percent_race_team_aggressive_v1 - error_near_miss_distribution](no_source_year_percent_race_team_aggressive_v1/error_near_miss_distribution.png)
![P5 percent_race_team_aggressive_v1 - error_batch_fp_tp_feature_profiles](no_source_year_percent_race_team_aggressive_v1/error_batch_fp_tp_feature_profiles.png)


<!-- END AUTO_SECTION_14 -->
