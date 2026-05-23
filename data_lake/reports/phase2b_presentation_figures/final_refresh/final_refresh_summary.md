# Final Refresh Summary

## What changed
- Built final presentation package from current corrected pit_success_h2 artifacts only.
- Main pit_success comparison now uses strict operational precision (no-match positives are FP).
- Matched-pit success rate is retained only in SDE diagnostic outputs.

## Authoritative inputs
- SDE aggregate: `/home/pitesse/Desktop/Thesis/data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_aggregate_2022_2025.csv`
- Batch recommended/frontier: `/home/pitesse/Desktop/Thesis/data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv`, `/home/pitesse/Desktop/Thesis/data_lake/reports/ml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv`
- MOA recommended/frontier: `/home/pitesse/Desktop/Thesis/data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv`, `/home/pitesse/Desktop/Thesis/data_lake/reports/sml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv`
- Runtime table: `/home/pitesse/Desktop/Thesis/data_lake/reports/phase2b_timing/training_runtime_2022_2025.csv`
- Canonical universe: `/home/pitesse/Desktop/Thesis/data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv` + pit-truth eligibility audits 2022-2025

## pit_success slide guidance
- Main comparison: use strict_precision + successful_event_coverage + F0.5.
- SDE diagnostic slide: show matched_pit_success_rate next to strict_precision with explicit caveat.
- Never label matched-pit success rate as precision in the main comparison.

## Difference to explain orally
- matched-pit success rate = quality conditional on matched known pit outcomes (diagnostic).
- strict precision = TP / (TP + FP_no_match + FP_failure) (operational, comparable with learners).

## Replacement figures
- `/home/pitesse/Desktop/Thesis/data_lake/reports/phase2b_presentation_figures/final_refresh/pit_any_headline_comparison.png`
- `/home/pitesse/Desktop/Thesis/data_lake/reports/phase2b_presentation_figures/final_refresh/pit_success_apples_to_apples_main.png`
- `/home/pitesse/Desktop/Thesis/data_lake/reports/phase2b_presentation_figures/final_refresh/pit_success_sde_diagnostic_operational_vs_matched.png`
- `/home/pitesse/Desktop/Thesis/data_lake/reports/phase2b_presentation_figures/final_refresh/training_runtime_comparison.png`
- `/home/pitesse/Desktop/Thesis/data_lake/reports/phase2b_presentation_figures/final_refresh/race_level_kappa_diagnostics.png`
- `/home/pitesse/Desktop/Thesis/data_lake/reports/phase2b_presentation_figures/final_refresh/race_level_gmean_diagnostics.png`
