# pit_any Final Recap

| System | learning_mode | target | selected_threshold | precision | event_recall | F0.5 | AP | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Flink Strategy Engine | deterministic streaming rule | pit_any_h2 | rule | 0.270574 | 0.071194 | 0.173434 |  | Rule baseline (no score threshold). |
| Batch No-Year | offline batch ML (OOF) | pit_any_h2 | 0.200000 | 0.204449 | 0.122047 | 0.180126 | 0.131343 | selected operating point |
| Batch Percent | offline batch ML (OOF) | pit_any_h2 | 0.190000 | 0.241796 | 0.219816 | 0.237055 | 0.190228 | selected operating point |
| MOA No-Year | online MOA prequential | pit_any_h2 | 0.530000 | 0.349584 | 0.183399 | 0.295950 | 0.160907 | selected operating point |
| MOA Percent | online MOA prequential | pit_any_h2 | 0.530000 | 0.344118 | 0.182087 | 0.292127 | 0.195114 | selected operating point |
