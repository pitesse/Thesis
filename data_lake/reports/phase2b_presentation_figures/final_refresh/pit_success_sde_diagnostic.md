# pit_success SDE Diagnostic

| System | positive_call_definition | predicted_positives | matched_known_pits | matched_success | matched_failure | matched_unknown | matched_pit_success_rate | strict_precision | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Flink Strategy Engine | PIT_NOW | 1235 | 151 | 88 | 63 | 135 | 0.582781 | 0.080000 | matched_pit_success_rate is diagnostic only |
| Flink Strategy Engine | PIT_NOW + GOOD_PIT | 5540 | 495 | 311 | 184 | 347 | 0.628283 | 0.059888 | matched_pit_success_rate is diagnostic only |
