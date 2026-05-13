# SDE Results Comparison (Dual-Contract + Truth Lenses)

Generated at (UTC): 2026-05-11T12:40:29.094750+00:00

Contracts:
- `pit_any_h2` strict timing: `episode_level + PIT_NOW_ONLY + H=2`
- `pit_success_h2` strategy advisor: `row_level + PIT_NOW_PLUS_GOOD_PIT + H=2`

Truth lenses: `raw`, `clean_actionable`, `clean_dry_strategy`.

Lens positioning for thesis reporting:
- `raw`: conservative official lens (all eligible pit events).
- `clean_actionable`: main fair diagnostic lens (primary cleaned evaluation).
- `clean_dry_strategy`: stricter sensitivity lens (appendix), not the primary cleaned headline.

## 0) Validity Cleanup Timeline

- Initial merged comparator artifacts exposed invalid race-universe contamination: the SDE-side raw comparator covered 96 races, exceeding the expected 2022-2025 race universe and indicating ghost/duplicated race coverage.
- Additional temporal-risk checks showed that some event information could be carried forward/backward across the stream, creating a risk that SDE, Batch ML, and SML/MOA were partially benefiting from future or invalid-context information.
- Because this affected all paradigms, the early full-feature / old-master results are now treated as provenance and ablation context, not final headline evidence.
- Explainability then flagged `_source_year` as a dominant non-causal driver in earlier Batch and MOA surrogate settings.
- We therefore removed `_source_year`, rebuilt no-year profiles, added percentage/race-progress features, and re-evaluated under a stricter dual-contract setup.
- Final deterministic SDE was frozen as `c6_cfg120_fixed`.
- Final ML/MOA headline protocol is Phase 2B under `canonical_sde_truth`, with `clean_actionable` as the main fairness lens.
- `raw` and `clean_dry_strategy` remain sensitivity lenses, not headline substitutes.

## 1) 2025 Holdout Comparison (C123 vs C5 vs C6)

### 1.1 `pit_any_h2`
| Variant | Truth Lens | Row TP | TP(for recall) | FP | FN | Precision | Recall | F0.5 | Eligible Actual Pits | TP(for recall)+FN==Eligible | RowTP+FN==Eligible |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| c123 | clean_actionable | 54 | 54 | 179 | 742 | 0.231760 | 0.067839 | 0.156250 | 796 | True | True |
| c5 | clean_actionable | 77 | 77 | 277 | 719 | 0.217514 | 0.096734 | 0.174051 | 796 | True | True |
| c6_cfg120_fixed | clean_actionable | 63 | 63 | 191 | 733 | 0.248031 | 0.079146 | 0.173841 | 796 | True | True |
| c123 | clean_dry_strategy | 54 | 53 | 179 | 655 | 0.231760 | 0.074859 | 0.163304 | 708 | True | False |
| c5 | clean_dry_strategy | 77 | 74 | 277 | 634 | 0.217514 | 0.104520 | 0.178845 | 708 | True | False |
| c6_cfg120_fixed | clean_dry_strategy | 63 | 60 | 191 | 648 | 0.248031 | 0.084746 | 0.179038 | 708 | True | False |
| c123 | raw | 54 | 54 | 179 | 753 | 0.231760 | 0.066914 | 0.155262 | 807 | True | True |
| c5 | raw | 77 | 77 | 277 | 730 | 0.217514 | 0.095415 | 0.173189 | 807 | True | True |
| c6_cfg120_fixed | raw | 63 | 63 | 191 | 744 | 0.248031 | 0.078067 | 0.172792 | 807 | True | True |

### 1.2 `pit_success_h2` (Strategy Lens by Truth Lens)
| Variant | Truth Lens | Precision | TP | FP | Scored | NO_MATCH |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| c123 | clean_actionable | 0.606061 | 80 | 52 | 132 | 1,438 |
| c5 | clean_actionable | 0.606299 | 77 | 50 | 127 | 1,465 |
| c6_cfg120_fixed | clean_actionable | 0.606061 | 80 | 52 | 132 | 1,448 |
| c123 | clean_dry_strategy | 0.606061 | 80 | 52 | 132 | 1,438 |
| c5 | clean_dry_strategy | 0.606299 | 77 | 50 | 127 | 1,465 |
| c6_cfg120_fixed | clean_dry_strategy | 0.606061 | 80 | 52 | 132 | 1,448 |
| c123 | raw | 0.606061 | 80 | 52 | 132 | 1,438 |
| c5 | raw | 0.606299 | 77 | 50 | 127 | 1,465 |
| c6_cfg120_fixed | raw | 0.606061 | 80 | 52 | 132 | 1,448 |

## 2) Final SDE (`c6_cfg120_fixed`) Aggregate 2022-2025

### 2.1 `pit_any_h2`
| Truth Lens | Row TP | TP(for recall) | FP | FN | Precision | Recall | F1 | F0.5 | Eligible Actual Pits | TP(for recall)+FN==Eligible | RowTP+FN==Eligible |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| clean_actionable | 217 | 217 | 585 | 2,831 | 0.270574 | 0.071194 | 0.112727 | 0.173434 | 3,048 | True | True |
| clean_dry_strategy | 217 | 193 | 585 | 2,541 | 0.270574 | 0.070593 | 0.111972 | 0.172716 | 2,734 | True | False |
| raw | 217 | 217 | 585 | 3,087 | 0.270574 | 0.065678 | 0.105699 | 0.166615 | 3,304 | True | True |

### 2.2 `pit_success_h2` (by Truth Lens)
| Truth Lens | Precision | TP | FP | Scored | NO_MATCH |
| --- | ---: | ---: | ---: | ---: | ---: |
| clean_actionable | 0.628283 | 311 | 184 | 495 | 4,698 |
| clean_dry_strategy | 0.628283 | 311 | 184 | 495 | 4,698 |
| raw | 0.628283 | 311 | 184 | 495 | 4,698 |

## 3) Delta vs Post-C123 Baseline (Official Raw Lens)

- C123 raw pit_any: TP=177, FP=530, FN=3,127, precision=0.250354, recall=0.053571, F0.5=0.144325
- C6 raw pit_any: TP=217, FP=585, FN=3,087, precision=0.270574, recall=0.065678, F0.5=0.166615
- Delta: TP=+40, FP=+55, FN=-40, precision=0.020220, recall=0.012107, F0.5=0.022291
- C123 pit_success precision=0.632323 vs C6 pit_success precision=0.628283

## 4) Invariant Note

- For `pit_any_h2`, precision is computed over emitted `PIT_NOW` rows/episodes, while recall is computed over unique eligible actual pit events.
- Under clean truth lenses, a row-level matched pit may be removed from the recall-eligible truth set if that pit belongs to an excluded non-actionable category.
- Strict comparator invariant: `TP(for recall)+FN == Eligible Actual Pits`.
- `RowTP+FN==Eligible` may be false under clean lenses for the reason above.
- Practical implication here: the earlier expected clean-dry recall lift is reduced once event-level eligibility is enforced (`Row TP` and `TP(for recall)` diverge under `clean_dry_strategy`).

## 5) Final SDE Verdict

- Final deterministic SDE is frozen as `c6_cfg120_fixed`.
- Main fair diagnostic lens is `clean_actionable`.
- `clean_dry_strategy` is retained as sensitivity analysis only.

## 6) Source Artifacts

- `data_lake/reports/sde_dual_contract_raw_vs_clean_2025_with_deltas.csv`
- `data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_per_year_2022_2025.csv`
- `data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_aggregate_2022_2025.csv`
- `data_lake/reports/sde_pit_any_diagnostics_2022_2025_post_c123.csv`
- `data_lake/reports/pit_truth_eligibility_audit_{2022..2025}_c6_cfg120_fixed.csv`

## 6.1) Phase 2B Naming Note

| Internal name | Report label | Meaning |
| --- | --- | --- |
| c6_cfg120_fixed | Final SDE | frozen deterministic rule baseline |
| e0_no_source_year | No-Year Baseline | profile with `_source_year` removed |
| p1_percent_conservative_v1 | Percent Features | conservative percentage/race-progress feature profile |
| Batch E0 | Batch No-Year | Batch ML with No-Year Baseline |
| Batch P1 | Batch Percent | Batch ML with Percent Features |
| SML/MOA E0 | MOA No-Year | streaming MOA with No-Year Baseline |
| SML/MOA P1 | MOA Percent | streaming MOA with Percent Features |

## 7) Batch ML Phase 2B (Canonical SDE Truth Headline)

Universe contract for Phase 2B headline:
- Universe mode: `canonical_sde_truth`
- Race/driver universe file: `data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv`
- Truth events: `pit_truth_eligibility_audit_{2022..2025}_c6_cfg120_fixed.csv`
- Native universe and ML/SDE shared intersection are sensitivity diagnostics only.

Canonical denominators (fixed across SDE/Batch in headline mode):
- `raw = 3304`
- `clean_actionable = 3048`
- `clean_dry_strategy = 2734`

### 7.1 Universe Alignment Snapshot

| Universe | Race/Driver Pairs |
| --- | ---: |
| ML OOF | 1,764 |
| SDE variant | 1,691 |
| Shared intersection (audit only) | 1,671 |

### 7.2 `pit_any_h2` (Main Fair Lens: `clean_actionable`)

| Profile | Threshold | AP | row_tp | tp_for_recall | FP | FN | Scored | Precision | Recall | F0.5 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Batch No-Year | 0.20 | 0.131343 | 386 | 372 | 1,502 | 2,676 | 1,888 | 0.204449 | 0.122047 | 0.180126 |
| Batch Percent | 0.19 | 0.190228 | 700 | 670 | 2,195 | 2,378 | 2,895 | 0.241796 | 0.219816 | 0.237055 |
| Delta (Batch Percent - Batch No-Year) | — | +0.058885 | +314 | +298 | +693 | -298 | +1,007 | +0.037347 | +0.097769 | +0.056929 |

Interpretation:
- Batch Percent is the best Batch profile for `pit_any_h2 clean_actionable` in Phase 2B headline mode.
- Gains are simultaneous on AP, scored reach, recall, and F0.5, with higher precision at the selected threshold.

### 7.3 `pit_success_h2` (Main Fair Lens: `clean_actionable`)

| Profile | Threshold | AP | row_tp | tp_for_recall | FP | FN | Scored | Precision | Recall | F0.5 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Batch No-Year | 0.19 | 0.062195 | 64 | 107 | 25 | 2,941 | 89 | 0.719101 | 0.035105 | 0.146849 |
| Batch Percent | 0.19 | 0.080373 | 114 | 237 | 69 | 2,811 | 183 | 0.622951 | 0.077756 | 0.259312 |
| Delta (Batch Percent - Batch No-Year) | — | +0.018178 | +50 | +130 | +44 | -130 | +94 | -0.096150 | +0.042651 | +0.112462 |

Interpretation:
- Batch No-Year remains the conservative high-precision guardrail.
- Batch Percent provides broader strategy reach and materially stronger F0.5/recall.

### 7.4 Metric Semantics Reminder

- `row_tp` is row-level TP used in precision.
- `tp_for_recall` is event-level TP used in recall (`tp_for_recall / eligible_actual_pit_count`).
- Under clean lenses, these can diverge by design.

### 7.5 Phase 2B Artifacts

- Headline recommended points:
  - `data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv`
- Headline frontier:
  - `data_lake/reports/ml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv`
  - `data_lake/reports/ml_phase2b_dual_contract_2022_2025/by_year/phase2b_threshold_frontier_by_year.csv`
  - `data_lake/reports/ml_phase2b_dual_contract_2022_2025/phase2b_threshold_frontier_report.md`
- Compact No-Year vs Percent canonical comparison:
  - `data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_e0_vs_p1_canonical_compact.csv`

## 8) SML/MOA Phase 2B (Canonical SDE Truth Headline)

Universe contract for SML headline evaluation:
- Universe mode: `canonical_sde_truth`
- Race/driver universe: `data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv`
- Truth events: `pit_truth_eligibility_audit_{2022..2025}_c6_cfg120_fixed.csv`
- Native universe remains sensitivity-only (non-headline).

Canonical denominators (fixed):
- `raw = 3304`
- `clean_actionable = 3048`
- `clean_dry_strategy = 2734`

### 8.1 `pit_any_h2` (Main Fair Lens: `clean_actionable`)

| Profile | Threshold | AP | row_tp | tp_for_recall | FP | FN | Scored | Precision | Recall | F0.5 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MOA No-Year | 0.53 | 0.160907 | 588 | 559 | 1,094 | 2,489 | 1,682 | 0.349584 | 0.183399 | 0.295950 |
| MOA Percent | 0.53 | 0.195114 | 585 | 555 | 1,115 | 2,493 | 1,700 | 0.344118 | 0.182087 | 0.292127 |

Interpretation:
- With continuous MOA vote-score frontiers, MOA No-Year is the slightly more conservative streaming pick for `pit_any_h2 clean_actionable`, with marginally higher precision, recall, and F0.5 at the selected operating point.
- MOA Percent still has higher AP, indicating better overall ranking/discrimination, but its selected headline decision operating point is slightly behind MOA No-Year.

### 8.2 `pit_success_h2` (Main Fair Lens: `clean_actionable`)

| Profile | Threshold | AP | row_tp | tp_for_recall | FP | FN | Scored | Precision | Recall | F0.5 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MOA No-Year | 0.56 | 0.112108 | 38 | 51 | 4 | 2,997 | 42 | 0.904762 | 0.016732 | 0.077899 |
| MOA Percent | 0.55 | 0.125342 | 66 | 85 | 14 | 2,963 | 80 | 0.825000 | 0.027887 | 0.122828 |

Interpretation:
- MOA No-Year remains the strict high-precision guardrail for `pit_success_h2`.
- MOA Percent provides broader strategy reach with higher recall, higher F0.5, and higher AP, at the cost of lower precision.

### 8.3 `pit_success_h2` Operating-Policy Sensitivity (Why `0.55` Looks Conservative)

`phase2b_threshold_frontier` currently uses:
- `pit_any_h2`: `max F0.5` under minimum scored constraint.
- `pit_success_h2`: `max precision` under minimum scored constraint.

So MOA `pit_success_h2` selected thresholds near `0.55` are **policy-consistent high-precision guardrails**, not a threshold-sort bug.

Clean-actionable operating-policy sensitivity table (`canonical_sde_truth`):

| Model | Policy point | Threshold | Precision | row_tp | truth events covered | FP | Scored | Recall | F0.5 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Final SDE | Final SDE operating point | n/a | 0.628283 | 311 | 311 | 184 | 495 | 0.102034 | 0.309268 |
| Batch No-Year | Selected Phase 2B (precision-first) | 0.19 | 0.719101 | 64 | 107 | 25 | 89 | 0.035105 | 0.146849 |
| Batch No-Year | Max F0.5 sensitivity | 0.05 | 0.592784 | 460 | 1094 | 316 | 776 | 0.358924 | 0.524443 |
| Batch Percent | Selected Phase 2B (precision-first) | 0.19 | 0.622951 | 114 | 237 | 69 | 183 | 0.077756 | 0.259312 |
| Batch Percent | Max F0.5 sensitivity | 0.05 | 0.608867 | 618 | 1472 | 397 | 1015 | 0.482940 | 0.578688 |
| MOA No-Year | Selected Phase 2B (precision-first) | 0.56 | 0.904762 | 38 | 51 | 4 | 42 | 0.016732 | 0.077899 |
| MOA No-Year | Precision closest to Final SDE | 0.24 | 0.626674 | 1076 | 2509 | 641 | 1717 | 0.823163 | 0.658092 |
| MOA Percent | Selected Phase 2B (precision-first) | 0.55 | 0.825000 | 66 | 85 | 14 | 80 | 0.027887 | 0.122828 |
| MOA Percent | Precision closest to Final SDE | 0.29 | 0.630088 | 1068 | 2473 | 627 | 1695 | 0.811352 | 0.659559 |

Interpretation:
- Official headline for `pit_success_h2` stays precision-first (strategy-advice guardrail), not blind max-F0.5.
- Batch sensitivity indicates a smaller policy gain and supports a representation-limitation interpretation for many SDE-style successes.
- At the selected precision-first point, MOA behaves as a strict guardrail.
- At SDE-equivalent precision, MOA recovers much larger `pit_success_h2` reach and event coverage.
- For MOA, the apparent weakness in `pit_success_h2` was mainly an operating-policy choice, not missing predictive signal.
- Artifact-only score diagnostics support this split:
  - Batch `pit_success_h2` looks more score-limited/representation-limited.
  - MOA `pit_success_h2` looks primarily threshold-policy-limited at the selected guardrail point.
- Cross-method caveat: Batch-vs-MOA row universes are not identical in this workspace (`MOA-only +1007` keys under canonical race/driver filtering), so absolute cross-method coverage counts should be interpreted with that source-integrity caveat.

Supporting artifacts:
- `data_lake/reports/phase2b_presentation_figures/phase2b_operating_policy_audit.md`
- `data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_threshold_diagnostic.csv`
- `data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_threshold_diagnostic.md`
- `data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_batch_vs_moa_score_diagnostic.csv`
- `data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_batch_vs_moa_score_diagnostic.md`
- `data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_score_distribution_batch_vs_moa_clean_actionable.png`
- `data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_operating_policy_sensitivity_batch_moa_clean_actionable.png`

### 8.4 SML Score-Mode Caveat

- Current Phase 2B SML run uses continuous MOA vote-score outputs (`raw_proba`) with a continuous MOA vote-score frontier.
- `calibrated_proba` is currently an uncalibrated passthrough of `raw_proba`, so calibration/reliability plots remain diagnostics rather than proof of calibrated probability quality.
- The previous fixed-threshold (`0.05`) hard-decision style numbers are retained only as historical operating-point diagnostics and are not the final headline SML operating points.

### 8.5 SML Phase 2B Artifacts

Headline artifacts:
- `data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv`
- `data_lake/reports/sml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv`
- `data_lake/reports/sml_phase2b_dual_contract_2022_2025/by_year/phase2b_threshold_frontier_by_year.csv`
- `data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_e0_vs_p1_canonical_compact.csv`
- `data_lake/reports/sml_phase2b_dual_contract_2022_2025/prequential/sml_phase2b_preq_summary.csv`

Sensitivity-only artifacts:
- `data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points_native.csv`
- `data_lake/reports/sml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact_native.csv`
- `data_lake/reports/sml_phase2b_dual_contract_2022_2025/by_year/phase2b_threshold_frontier_by_year_native.csv`

Cross-model pit_success threshold diagnostic:
- `data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_threshold_diagnostic.csv`
- `data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_threshold_diagnostic.md`
- `data_lake/reports/phase2b_presentation_figures/phase2b_operating_policy_audit.md`

## 9) Final Phase 2B Figure Pack / Delivery Artifacts (Corrected Headline)

Figure pack root:
- `data_lake/reports/phase2b_presentation_figures/`
- PNG/PDF/SVG versions are generated for presentation and thesis export.

Batch probability-diagnostic source integrity:
- Batch PR/calibration/PR-Gain/decision diagnostics use Phase 2B OOF when available.
- In this workspace, Phase 2B diagnostics use `data_lake/reports/ml_phase2a_dual_contract_2022_2025/oof` as the **validated OOF probability-diagnostic source** for the Phase 2B dual-contract reporting layer, with explicit source-integrity checks in delivery audit.

### 9.1 Headline Comparator Families

![Phase2B Clean Actionable Metrics](data_lake/reports/phase2b_presentation_figures/phase2b_clean_actionable_precision_recall_f05.png)

![Phase2B Clean Actionable Scored TP FP](data_lake/reports/phase2b_presentation_figures/phase2b_clean_actionable_scored_tp_fp.png)

![Phase2B Pit Success Operating-Policy Sensitivity](data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_operating_policy_sensitivity_clean_actionable.png)

![Phase2B Pit Success Operating-Policy Sensitivity (Batch + MOA)](data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_operating_policy_sensitivity_batch_moa_clean_actionable.png)

### 9.2 PR Curve Families

![Phase2B Batch PR Curves](data_lake/reports/phase2b_presentation_figures/phase2b_batch_pr_curves_clean_actionable.png)

![Phase2B Batch PR Curves By Year](data_lake/reports/phase2b_presentation_figures/phase2b_batch_pr_curves_by_year_clean_actionable.png)

![Phase2B MOA PR Curves](data_lake/reports/phase2b_presentation_figures/phase2b_sml_pr_curves_clean_actionable.png)

![Phase2B MOA PR Curves By Year](data_lake/reports/phase2b_presentation_figures/phase2b_sml_pr_curves_by_year_clean_actionable.png)

### 9.3 Threshold Frontier Families

![Phase2B Batch Threshold Frontier](data_lake/reports/phase2b_presentation_figures/phase2b_threshold_frontier_batch_clean_actionable.png)

![Phase2B MOA Threshold Frontier](data_lake/reports/phase2b_presentation_figures/phase2b_threshold_frontier_sml_clean_actionable.png)

### 9.4 Probability Diagnostics (Methodological)

![Phase2B Batch Calibration](data_lake/reports/phase2b_presentation_figures/phase2b_calibration_batch_clean_actionable.png)

![Phase2B MOA Calibration](data_lake/reports/phase2b_presentation_figures/phase2b_calibration_sml_clean_actionable.png)

![Phase2B Batch PR-Gain](data_lake/reports/phase2b_presentation_figures/phase2b_pr_gain_batch_clean_actionable.png)

![Phase2B MOA PR-Gain](data_lake/reports/phase2b_presentation_figures/phase2b_pr_gain_sml_clean_actionable.png)

![Phase2B Batch Decision Curve](data_lake/reports/phase2b_presentation_figures/phase2b_decision_curve_batch_clean_actionable.png)

![Phase2B MOA Decision Curve](data_lake/reports/phase2b_presentation_figures/phase2b_decision_curve_sml_clean_actionable.png)

Caveat:
- MOA vote scores are continuous diagnostics; `calibrated_proba` is currently an **uncalibrated passthrough**.

### 9.5 Temporal / By-Year Families

![Phase2B Per-Year Metrics](data_lake/reports/phase2b_presentation_figures/phase2b_per_year_precision_recall_f05_clean_actionable.png)

![Phase2B Temporal Drift](data_lake/reports/phase2b_presentation_figures/phase2b_temporal_drift_by_race_clean_actionable.png)

![Phase2B Temporal Drift Slide](data_lake/reports/phase2b_presentation_figures/phase2b_temporal_drift_by_race_clean_actionable_slide.png)

### 9.6 Explainability Families

Batch direct-SHAP:

![Phase2B Batch E0 SHAP Global](data_lake/reports/phase2b_presentation_figures/phase2b_batch_e0_shap_global_bar.png)

![Phase2B Batch E0 SHAP Beeswarm](data_lake/reports/phase2b_presentation_figures/phase2b_batch_e0_shap_beeswarm.png)

![Phase2B Batch P1 SHAP Global](data_lake/reports/phase2b_presentation_figures/phase2b_batch_p1_shap_global_bar.png)

![Phase2B Batch P1 SHAP Beeswarm](data_lake/reports/phase2b_presentation_figures/phase2b_batch_p1_shap_beeswarm.png)

![Phase2B Batch SHAP Top Feature Comparison](data_lake/reports/phase2b_presentation_figures/phase2b_batch_shap_top_feature_comparison_e0_p1.png)

SML/MOA surrogate-SHAP:

![Phase2B MOA E0 Surrogate SHAP Global](data_lake/reports/phase2b_presentation_figures/phase2b_sml_e0_surrogate_shap_global_bar.png)

![Phase2B MOA P1 Surrogate SHAP Global](data_lake/reports/phase2b_presentation_figures/phase2b_sml_p1_surrogate_shap_global_bar.png)

![Phase2B MOA Surrogate SHAP Top Feature Comparison](data_lake/reports/phase2b_presentation_figures/phase2b_sml_surrogate_shap_top_feature_comparison_e0_p1.png)

![Phase2B MOA Surrogate Fidelity Summary](data_lake/reports/phase2b_presentation_figures/phase2b_sml_surrogate_fidelity_summary.png)

### 9.7 Corrected Phase 2B Error-Diagnostic Families

![Phase2B Error Consensus Upset](data_lake/reports/phase2b_presentation_figures/phase2b_error_consensus_upset_clean_actionable.png)

![Phase2B Error Near Miss Distribution](data_lake/reports/phase2b_presentation_figures/phase2b_error_near_miss_distribution_clean_actionable.png)

![Phase2B Error Batch FP TP Feature Profiles](data_lake/reports/phase2b_presentation_figures/phase2b_error_batch_fp_tp_feature_profiles_clean_actionable.png)

![Phase2B Error MOA FP TP Feature Profiles](data_lake/reports/phase2b_presentation_figures/phase2b_error_sml_fp_tp_feature_profiles_clean_actionable.png)

## 10) Paper-Inspired Diagnostics (Kappa)

These figures follow time-evolving analytics style (paper-inspired), while preserving our own corrected artifact definitions.

Definition guardrails:
- `Batch/MOA` kappa here is decision-level row classification agreement (`target_y` vs thresholded score).
- `Final SDE` is event-level comparator-centric; direct row-level kappa deltas versus SDE are diagnostic-only and explicitly caveated.
- MOA prequential kappa remains a stream diagnostic and is shown separately.

Artifacts:
- `data_lake/reports/phase2b_presentation_figures/phase2b_kappa_summary_clean_actionable.csv`
- `data_lake/reports/phase2b_presentation_figures/phase2b_kappa_boxplot_by_model_family_clean_actionable.png`
- `data_lake/reports/phase2b_presentation_figures/phase2b_kappa_trajectory_by_year_or_race_clean_actionable.png`
- `data_lake/reports/phase2b_presentation_figures/phase2b_kappa_delta_vs_final_sde_clean_actionable.png`

![Phase2B Kappa Boxplot by Model Family](data_lake/reports/phase2b_presentation_figures/phase2b_kappa_boxplot_by_model_family_clean_actionable.png)

![Phase2B Kappa Trajectory by Year](data_lake/reports/phase2b_presentation_figures/phase2b_kappa_trajectory_by_year_or_race_clean_actionable.png)

![Phase2B Kappa Delta vs Final SDE](data_lake/reports/phase2b_presentation_figures/phase2b_kappa_delta_vs_final_sde_clean_actionable.png)

![Phase2B MOA Prequential Accuracy Kappa](data_lake/reports/phase2b_presentation_figures/phase2b_preq_accuracy_kappa.png)

## 11) Historical Ablation Graph Atlas (Appendix Pointer)

Historical/provenance graph families from the older report are catalogued separately in:
- `data_lake/reports/phase2b_presentation_figures/phase2b_historical_ablation_graph_atlas.md`

The main thesis headline evidence remains the corrected Phase 2B figures in Sections 7-10 above.
