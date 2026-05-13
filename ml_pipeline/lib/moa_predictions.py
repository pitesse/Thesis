"""Utilities to decode MOA prediction files into binary labels and optional scores."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _decode_core(
    pred_path: Path,
    y_true: pd.Series,
    *,
    pred_column: int = 0,
    true_column: int = 1,
    min_mapping_purity: float = 0.99,
    require_aligned_rows: bool = True,
) -> dict[str, Any]:
    raw = pd.read_csv(pred_path, header=None)
    max_column = max(pred_column, true_column)
    if raw.shape[1] <= max_column:
        raise ValueError(
            f"MOA predictions file must contain at least {max_column + 1} columns"
        )

    pred_code = pd.to_numeric(raw.iloc[:, pred_column], errors="coerce")
    true_code = pd.to_numeric(raw.iloc[:, true_column], errors="coerce")

    y_len = int(len(y_true))
    pred_len = int(len(pred_code))
    true_len = int(len(true_code))
    if require_aligned_rows and not (y_len == pred_len == true_len):
        raise ValueError(
            "MOA prediction length mismatch: "
            f"target_rows={y_len}, pred_rows={pred_len}, true_rows={true_len}"
        )

    n = y_len if require_aligned_rows else min(y_len, pred_len, true_len)
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

    mapped_pred = pred_code.map(code_map).astype("Int64")
    return {
        "raw": raw,
        "source_format": "legacy_pred_two_cols_or_votes",
        "n": int(n),
        "pred_code": pred_code,
        "true_code": true_code,
        "mapped_pred": mapped_pred,
        "code_map": code_map,
        "purity_map": purity_map,
        "pred_column": int(pred_column),
        "true_column": int(true_column),
    }


def _build_common_diagnostics(core: dict[str, Any]) -> dict[str, Any]:
    mapped_pred = core["mapped_pred"]
    raw = core["raw"]
    diagnostics: dict[str, Any] = {
        "rows_aligned": int(core["n"]),
        "prediction_column_count": int(raw.shape[1]),
        "code_map": {str(k): int(v) for k, v in core["code_map"].items()},
        "code_map_purity": {str(k): float(v) for k, v in core["purity_map"].items()},
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
    return diagnostics


def decode_moa_predictions(
    pred_path: Path,
    y_true: pd.Series,
    *,
    pred_column: int = 0,
    true_column: int = 1,
    min_mapping_purity: float = 0.99,
) -> tuple[pd.Series, dict[str, Any]]:
    core = _decode_core(
        pred_path=pred_path,
        y_true=y_true,
        pred_column=pred_column,
        true_column=true_column,
        min_mapping_purity=min_mapping_purity,
        require_aligned_rows=True,
    )
    diagnostics = _build_common_diagnostics(core)
    diagnostics["score_mode"] = "hard_label_only"
    diagnostics["score_is_hard_decision"] = True
    diagnostics["score_column_count"] = 0
    return core["mapped_pred"], diagnostics


def decode_moa_predictions_with_scores(
    pred_path: Path,
    y_true: pd.Series,
    *,
    pred_column: int = 0,
    true_column: int = 1,
    min_mapping_purity: float = 0.99,
) -> tuple[pd.Series, pd.Series, dict[str, Any]]:
    """Decode MOA predictions and attempt to infer positive-class scores.

    Returns:
    - mapped binary predictions in {0,1} via target-code mapping
    - score series in [0,1] when votes are available; otherwise hard-label fallback
    - diagnostics with score extraction mode details
    """

    # Preferred format: custom vote logger CSV with explicit columns.
    vote_logger = _decode_vote_logger_csv(
        pred_path=pred_path,
        y_true=y_true,
        min_mapping_purity=min_mapping_purity,
    )
    if vote_logger is not None:
        return vote_logger

    core = _decode_core(
        pred_path=pred_path,
        y_true=y_true,
        pred_column=pred_column,
        true_column=true_column,
        min_mapping_purity=min_mapping_purity,
        require_aligned_rows=True,
    )
    diagnostics = _build_common_diagnostics(core)

    raw = core["raw"].iloc[: core["n"]].reset_index(drop=True)
    code_map: dict[float, int] = core["code_map"]
    mapped_pred: pd.Series = core["mapped_pred"]
    pred_col = int(core["pred_column"])
    true_col = int(core["true_column"])

    non_label_cols = [i for i in range(raw.shape[1]) if i not in {pred_col, true_col}]
    sorted_codes = sorted(code_map.keys())
    positive_codes = [code for code, target in code_map.items() if int(target) == 1]
    negative_codes = [code for code, target in code_map.items() if int(target) == 0]

    score = pd.Series(np.nan, index=range(len(mapped_pred)), dtype=float)
    score_mode = "hard_label_only"
    score_source = "predicted_label_passthrough"
    score_is_hard_decision = True

    # Attempt vote-based score extraction if the prediction file exposes extra columns.
    vote_col_map: dict[float, int] = {}
    if len(non_label_cols) >= len(sorted_codes) and len(sorted_codes) >= 2 and len(positive_codes) == 1:
        assigned_vote_cols = non_label_cols[: len(sorted_codes)]
        vote_col_map = dict(zip(sorted_codes, assigned_vote_cols))
        vote_matrix = pd.DataFrame(
            {
                code: pd.to_numeric(raw.iloc[:, col], errors="coerce")
                for code, col in vote_col_map.items()
            }
        )
        denom = vote_matrix.sum(axis=1, skipna=True)
        positive_code = float(positive_codes[0])
        positive_vote = vote_matrix.get(positive_code, pd.Series(np.nan, index=vote_matrix.index))
        valid_denom = denom > 0
        score = pd.Series(np.nan, index=vote_matrix.index, dtype=float)
        score.loc[valid_denom] = (positive_vote.loc[valid_denom] / denom.loc[valid_denom]).astype(float)
        if score.notna().any():
            score_mode = "vote_normalized"
            score_source = "vote_1_over_vote_sum"
            score_is_hard_decision = False

    if score_mode == "hard_label_only":
        score = pd.to_numeric(mapped_pred, errors="coerce").fillna(0.0).astype(float)

    diagnostics["score_mode"] = score_mode
    diagnostics["score_source"] = score_source
    diagnostics["score_is_hard_decision"] = bool(score_is_hard_decision)
    diagnostics["score_column_count"] = int(len(non_label_cols))
    diagnostics["vote_column_map"] = {str(code): int(col) for code, col in vote_col_map.items()}
    diagnostics["positive_code_candidates"] = [float(code) for code in positive_codes]
    diagnostics["negative_code_candidates"] = [float(code) for code in negative_codes]
    diagnostics["score_known_rows"] = int(score.notna().sum())
    diagnostics["score_unknown_rows"] = int(score.isna().sum())
    diagnostics["score_unique_values"] = int(pd.Series(score).dropna().nunique())

    return mapped_pred, score, diagnostics


def _decode_vote_logger_csv(
    pred_path: Path,
    y_true: pd.Series,
    *,
    min_mapping_purity: float,
) -> tuple[pd.Series, pd.Series, dict[str, Any]] | None:
    raw = pd.read_csv(pred_path)
    required = {"row_index", "true_label", "predicted_label", "positive_score"}
    if not required.issubset(set(raw.columns)):
        return None

    y_true_num = pd.to_numeric(y_true, errors="coerce").reset_index(drop=True)
    if len(raw) != len(y_true_num):
        raise ValueError(
            "vote logger row count mismatch: "
            f"target_rows={len(y_true_num)}, prediction_rows={len(raw)}"
        )

    row_index = pd.to_numeric(raw["row_index"], errors="coerce")
    if row_index.isna().any():
        raise ValueError("vote logger contains non-numeric row_index values")
    row_index_int = row_index.astype(int).reset_index(drop=True)
    expected = pd.Series(range(len(row_index_int)), dtype=int)
    if not (row_index_int == expected).all():
        raise ValueError("vote logger row_index must be contiguous from 0..N-1")

    pred_code = pd.to_numeric(raw["predicted_label"], errors="coerce").reset_index(drop=True)
    true_code = pd.to_numeric(raw["true_label"], errors="coerce").reset_index(drop=True)

    valid = y_true_num.notna() & true_code.notna()
    mapping_frame = pd.DataFrame(
        {
            "true_code": true_code[valid],
            "target": y_true_num[valid].astype(int),
        }
    )
    if mapping_frame.empty:
        raise ValueError("cannot infer MOA code mapping, no valid true_label/target rows")

    code_map: dict[float, int] = {}
    purity_map: dict[float, float] = {}
    for code, grp in mapping_frame.groupby("true_code"):
        counts = grp["target"].value_counts(normalize=True)
        code_map[float(code)] = int(counts.index[0])
        purity_map[float(code)] = float(counts.iloc[0])

    low_purity = {str(k): v for k, v in purity_map.items() if v < min_mapping_purity}
    if low_purity:
        raise ValueError(
            "ambiguous MOA true-label mapping, purity below threshold: "
            f"{low_purity}, min_mapping_purity={min_mapping_purity}"
        )

    mapped_pred = pred_code.map(code_map).astype("Int64")
    score_base = pd.to_numeric(raw["positive_score"], errors="coerce")

    positive_codes = [code for code, target in code_map.items() if int(target) == 1]
    negative_codes = [code for code, target in code_map.items() if int(target) == 0]
    vote_cols = [col for col in raw.columns if str(col).startswith("vote_")]
    vote_col_map: dict[str, str] = {}
    for col in vote_cols:
        key = str(col).replace("vote_", "", 1)
        vote_col_map[key] = col

    score_mode = "provided_positive_score"
    score_source = "vote_logger_positive_score_column"
    score = score_base.copy()
    score_is_hard_decision = bool(score.dropna().nunique() <= 2)

    # If provided positive_score is hard/degenerate, try robust remap from vote_<code> columns.
    if (
        len(positive_codes) == 1
        and len(negative_codes) == 1
        and vote_col_map
        and (score_is_hard_decision or score.isna().all())
    ):
        pos_code = positive_codes[0]
        neg_code = negative_codes[0]
        pos_key_int = str(int(pos_code))
        neg_key_int = str(int(neg_code))
        pos_col = vote_col_map.get(pos_key_int, vote_col_map.get(str(float(pos_code))))
        neg_col = vote_col_map.get(neg_key_int, vote_col_map.get(str(float(neg_code))))
        if pos_col is not None and neg_col is not None:
            pos_vote = pd.to_numeric(raw[pos_col], errors="coerce")
            neg_vote = pd.to_numeric(raw[neg_col], errors="coerce")
            denom = pos_vote + neg_vote
            valid_denom = denom > 0
            remapped = pd.Series(np.nan, index=raw.index, dtype=float)
            remapped.loc[valid_denom] = (pos_vote.loc[valid_denom] / denom.loc[valid_denom]).astype(float)
            if remapped.notna().any():
                score = remapped
                score_mode = "vote_logger_code_mapped_votes"
                score_source = f"{pos_col}/({pos_col}+{neg_col})"
                score_is_hard_decision = bool(score.dropna().nunique() <= 2)

    if score.isna().all():
        score = pd.to_numeric(mapped_pred, errors="coerce").fillna(0.0).astype(float)
        score_mode = "hard_label_only"
        score_source = "predicted_label_passthrough"
        score_is_hard_decision = True

    diagnostics: dict[str, Any] = {
        "rows_aligned": int(len(raw)),
        "source_format": "vote_logger_csv",
        "prediction_column_count": int(raw.shape[1]),
        "code_map": {str(k): int(v) for k, v in code_map.items()},
        "code_map_purity": {str(k): float(v) for k, v in purity_map.items()},
        "unknown_prediction_rows": int(mapped_pred.isna().sum()),
        "known_prediction_rows": int(mapped_pred.notna().sum()),
        "score_mode": score_mode,
        "score_source": score_source,
        "score_is_hard_decision": bool(score_is_hard_decision),
        "score_column_count": int(len(vote_cols)),
        "score_unique_values": int(pd.Series(score).dropna().nunique()),
        "score_known_rows": int(score.notna().sum()),
        "score_unknown_rows": int(score.isna().sum()),
        "vote_columns_detected": vote_cols,
        "positive_code_candidates": [float(code) for code in positive_codes],
        "negative_code_candidates": [float(code) for code in negative_codes],
    }
    if mapped_pred.notna().any():
        known = mapped_pred.dropna().astype(int)
        diagnostics["known_positive_rate"] = float((known == 1).mean())
        diagnostics["known_negative_rate"] = float((known == 0).mean())
    else:
        diagnostics["known_positive_rate"] = float("nan")
        diagnostics["known_negative_rate"] = float("nan")

    return mapped_pred, score, diagnostics
