"""Utilities to decode MOA prediction files into binary labels safely."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def decode_moa_predictions(
    pred_path: Path,
    y_true: pd.Series,
    *,
    pred_column: int = 0,
    true_column: int = 1,
    min_mapping_purity: float = 0.99,
) -> tuple[pd.Series, dict[str, Any]]:
    raw = pd.read_csv(pred_path, header=None)
    max_column = max(pred_column, true_column)
    if raw.shape[1] <= max_column:
        raise ValueError(
            f"MOA predictions file must contain at least {max_column + 1} columns"
        )

    pred_code = pd.to_numeric(raw.iloc[:, pred_column], errors="coerce")
    true_code = pd.to_numeric(raw.iloc[:, true_column], errors="coerce")

    n = min(len(y_true), len(pred_code), len(true_code))
    y_true = pd.to_numeric(y_true.iloc[:n], errors="coerce")
    pred_code = pred_code.iloc[:n].reset_index(drop=True)
    true_code = true_code.iloc[:n].reset_index(drop=True)
    y_true = y_true.reset_index(drop=True)

    valid = y_true.notna() & true_code.notna()
    mapping_frame = pd.DataFrame(
        {
            "true_code": true_code[valid],
            "target": y_true[valid].astype(int),
        }
    )
    if mapping_frame.empty:
        raise ValueError("cannot infer MOA code mapping, no valid true_code/target pairs")

    code_map: dict[float, int] = {}
    purity_map: dict[float, float] = {}
    for code, grp in mapping_frame.groupby("true_code"):
        target_counts = grp["target"].value_counts(normalize=True)
        mode_target = int(target_counts.index[0])
        purity = float(target_counts.iloc[0])
        code_map[float(code)] = mode_target
        purity_map[float(code)] = purity

    low_purity = {str(k): v for k, v in purity_map.items() if v < min_mapping_purity}
    if low_purity:
        raise ValueError(
            "ambiguous MOA true-code mapping, purity below threshold: "
            f"{low_purity}, min_mapping_purity={min_mapping_purity}"
        )

    mapped_pred = pred_code.map(code_map)
    mapped_pred = mapped_pred.astype("Int64")

    diagnostics: dict[str, Any] = {
        "rows_aligned": int(n),
        "code_map": {str(k): int(v) for k, v in code_map.items()},
        "code_map_purity": {str(k): float(v) for k, v in purity_map.items()},
        "unknown_prediction_rows": int(mapped_pred.isna().sum()),
        "known_prediction_rows": int(mapped_pred.notna().sum()),
    }
    if mapped_pred.notna().any():
        known = mapped_pred.dropna().astype(int)
        diagnostics["known_positive_rate"] = float((known == 1).mean())
        diagnostics["known_negative_rate"] = float((known == 0).mean())
    else:
        diagnostics["known_positive_rate"] = float("nan")
        diagnostics["known_negative_rate"] = float("nan")

    return mapped_pred, diagnostics