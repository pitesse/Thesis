# pit_success Apples-to-Apples Main Comparison

| System | positive_call_definition | predicted_positives | TP | FP_no_match | FP_failure | unknown_excluded | strict_precision | successful_event_coverage | F0.5 | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Flink Strategy Engine | PIT_NOW | 1235 | 88 | 949 | 63 | 135 | 0.080000 | 0.083333 | 0.080645 | Main rule baseline. Matched-pit rate is diagnostic-only. |
| Batch No-Year | score>=0.37 | 46 | 11 | 31 | 4 | 0 | 0.239130 | 0.010417 | 0.044355 | selected operating point |
| Batch Percent | score>=0.51 | 51 | 10 | 36 | 5 | 0 | 0.196078 | 0.009470 | 0.039683 | selected operating point |
| MOA No-Year | score>=0.58 | 52 | 15 | 36 | 1 | 0 | 0.288462 | 0.014205 | 0.059335 | selected operating point |
| MOA Percent | score>=0.58 | 86 | 31 | 51 | 3 | 1 | 0.364706 | 0.029356 | 0.111032 | selected operating point |
