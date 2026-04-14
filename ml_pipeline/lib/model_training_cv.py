"""train pit strategy classifier with leakage-safe grouped cross validation.

Method notes:
- Grouped split by race follows Roberts et al. 2017 to avoid temporal leakage.
- Imbalanced objective follows Elkan 2001 via scale_pos_weight.
- Primary discrimination metric is PR-AUC, aligned with Saito and Rehmsmeier 2015.
- Brier score is reported for probability quality, consistent with calibration reporting practice.
"""

from __future__ import annotations

import argparse
from itertools import product
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss
from sklearn.model_selection import GroupKFold
from xgboost import XGBClassifier


DEFAULT_DATASET = "data_lake/ml_training_dataset.parquet"
DEFAULT_FOLDS = 5
DEFAULT_RANDOM_STATE = 42
DEFAULT_PROBA_THRESHOLD = 0.5
DEFAULT_SWEEP_MIN = 0.01
DEFAULT_SWEEP_MAX = 0.99
DEFAULT_SWEEP_POINTS = 99
DEFAULT_PRECISION_FLOOR = 0.10
DEFAULT_CONSTRAINED_FP_COST = 3.0
DEFAULT_CALIBRATION_POLICY = "auto"
DEFAULT_MIN_CALIBRATION_POSITIVES = 30
DEFAULT_OOF_OUTPUT = ""
DEFAULT_GRID_MAX_DELTA_STEP = (1,)
DEFAULT_GRID_SUBSAMPLE = (0.7,)
DEFAULT_GRID_COLSAMPLE_BYTREE = (0.8,)
DEFAULT_GRID_SCALE_POS_WEIGHT = ("30",)
DEFAULT_LEADERBOARD_TOP_K = 5
DEFAULT_LEADERBOARD_OUTPUT = ""

TARGET_COLUMN = "target_y"
GROUP_COLUMN = "race"

METADATA_DROP_COLUMNS = {
    "race",
    "driver",
    "lapNumber",
    "compound",
    "trackStatus",
    "matched_pit_lap",
    "matched_pit_result",
    "label_horizon_laps",
}


def _load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"dataset not found: {path}")

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"dataset is empty: {path}")
    return df


def _select_feature_columns(columns: Iterable[str]) -> list[str]:
    return [c for c in columns if c != TARGET_COLUMN and c not in METADATA_DROP_COLUMNS]


def _compute_scale_pos_weight(y: pd.Series) -> float:
    positives = int((y == 1).sum())
    negatives = int((y == 0).sum())
    if positives == 0:
        return 1.0
    return float(negatives / positives)


def _prepare_matrix(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, pd.Series, pd.DataFrame]:
    if TARGET_COLUMN not in df.columns:
        raise ValueError(f"missing target column: {TARGET_COLUMN}")
    if GROUP_COLUMN not in df.columns:
        raise ValueError(f"missing group column: {GROUP_COLUMN}")
    if "driver" not in df.columns:
        raise ValueError("missing metadata column: driver")
    if "lapNumber" not in df.columns:
        raise ValueError("missing metadata column: lapNumber")

    work = df.copy()

    work[TARGET_COLUMN] = pd.to_numeric(work[TARGET_COLUMN], errors="coerce")
    work = work[work[TARGET_COLUMN].isin([0, 1])].copy()
    work[TARGET_COLUMN] = work[TARGET_COLUMN].astype(int)

    work[GROUP_COLUMN] = work[GROUP_COLUMN].astype(str)

    feature_cols = _select_feature_columns(work.columns)
    if not feature_cols:
        raise ValueError("no feature columns remain after metadata drop")

    X_raw = work[feature_cols].copy()

    # One-hot encoding keeps split logic simple and deterministic for grouped CV.
    object_cols = X_raw.select_dtypes(
        include=["object", "category", "string"]
    ).columns.tolist()
    if object_cols:
        X = pd.get_dummies(X_raw, columns=object_cols, dummy_na=True)
    else:
        X = X_raw

    # XGBoost handles booleans, but casting is explicit for reproducibility.
    bool_cols = X.select_dtypes(include=["bool"]).columns
    if len(bool_cols) > 0:
        X[bool_cols] = X[bool_cols].astype(int)

    y = work[TARGET_COLUMN]
    groups = work[GROUP_COLUMN]
    meta = work[["race", "driver", "lapNumber"]].copy()
    meta["race"] = meta["race"].astype(str)
    meta["driver"] = meta["driver"].astype(str)
    meta["lapNumber"] = pd.to_numeric(meta["lapNumber"], errors="coerce").astype(
        "Int64"
    )

    return X, y, groups, meta


def _metrics_with_counts(
    y_true: pd.Series,
    proba: np.ndarray,
    threshold: float,
) -> tuple[float, float, float, int, int]:
    y_arr = np.asarray(y_true, dtype=int)
    pred = (np.asarray(proba, dtype=float) >= threshold).astype(int)

    tp = int(np.sum((pred == 1) & (y_arr == 1)))
    fp = int(np.sum((pred == 1) & (y_arr == 0)))
    fn = int(np.sum((pred == 0) & (y_arr == 1)))

    precision = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    recall = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    f1 = (
        float((2 * precision * recall) / (precision + recall))
        if (precision + recall) > 0
        else 0.0
    )
    return precision, recall, f1, tp, fp


def _metrics_at_threshold(
    y_true: pd.Series, proba: np.ndarray, threshold: float
) -> tuple[float, float, float]:
    precision, recall, f1, _, _ = _metrics_with_counts(y_true, proba, threshold)
    return precision, recall, f1


def _fit_isotonic_calibrator(
    scores: np.ndarray, y_true: pd.Series
) -> IsotonicRegression | None:
    y_arr = np.asarray(y_true, dtype=int)
    s_arr = np.asarray(scores, dtype=float)

    if np.unique(y_arr).size < 2:
        return None
    if np.unique(s_arr).size < 2:
        return None

    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(s_arr, y_arr)
    return calibrator


def _fit_sigmoid_calibrator(
    scores: np.ndarray, y_true: pd.Series
) -> LogisticRegression | None:
    y_arr = np.asarray(y_true, dtype=int)
    s_arr = np.asarray(scores, dtype=float)

    if np.unique(y_arr).size < 2:
        return None
    if np.unique(s_arr).size < 2:
        return None

    calibrator = LogisticRegression(solver="lbfgs", max_iter=1000)
    calibrator.fit(s_arr.reshape(-1, 1), y_arr)
    return calibrator


def _fit_calibrator(
    scores: np.ndarray,
    y_true: pd.Series,
    calibration_policy: str,
    min_calibration_positives: int,
) -> tuple[tuple[str, object] | None, str]:
    positives = int((y_true == 1).sum())
    negatives = int((y_true == 0).sum())

    if positives == 0 or negatives == 0:
        return None, "none"

    if calibration_policy == "isotonic":
        isotonic = _fit_isotonic_calibrator(scores, y_true)
        if isotonic is None:
            return None, "none"
        return ("isotonic", isotonic), "isotonic"

    if calibration_policy == "sigmoid":
        sigmoid = _fit_sigmoid_calibrator(scores, y_true)
        if sigmoid is None:
            return None, "none"
        return ("sigmoid", sigmoid), "sigmoid"

    if positives >= min_calibration_positives:
        isotonic = _fit_isotonic_calibrator(scores, y_true)
        if isotonic is not None:
            return ("isotonic", isotonic), "isotonic"

    sigmoid = _fit_sigmoid_calibrator(scores, y_true)
    if sigmoid is not None:
        return ("sigmoid", sigmoid), "sigmoid"

    isotonic = _fit_isotonic_calibrator(scores, y_true)
    if isotonic is not None:
        return ("isotonic", isotonic), "isotonic"

    return None, "none"


def _apply_calibrator(
    calibrator: tuple[str, object] | None, scores: np.ndarray
) -> np.ndarray:
    s_arr = np.asarray(scores, dtype=float)
    if calibrator is None:
        return s_arr

    kind, model = calibrator
    if kind == "isotonic":
        if not isinstance(model, IsotonicRegression):
            raise TypeError("invalid isotonic calibrator instance")
        return np.asarray(model.predict(s_arr), dtype=float)
    if kind == "sigmoid":
        if not isinstance(model, LogisticRegression):
            raise TypeError("invalid sigmoid calibrator instance")
        return np.asarray(model.predict_proba(s_arr.reshape(-1, 1))[:, 1], dtype=float)
    raise ValueError(f"unsupported calibrator type: {kind}")


def _find_best_f1_threshold_unconstrained(
    y_true: pd.Series,
    proba: np.ndarray,
    thresholds: np.ndarray,
) -> tuple[float, float, float, float]:
    best_threshold = float(thresholds[0])
    best_precision, best_recall, best_f1 = _metrics_at_threshold(
        y_true, proba, best_threshold
    )

    for threshold in thresholds[1:]:
        precision, recall, f1 = _metrics_at_threshold(y_true, proba, float(threshold))
        # tie-break favors higher recall under asymmetric false negative cost.
        if f1 > best_f1 or (np.isclose(f1, best_f1) and recall > best_recall):
            best_threshold = float(threshold)
            best_precision = precision
            best_recall = recall
            best_f1 = f1

    return best_threshold, best_precision, best_recall, best_f1


def _find_best_f1_threshold_with_precision_floor(
    y_true: pd.Series,
    proba: np.ndarray,
    thresholds: np.ndarray,
    precision_floor: float,
    constrained_fp_cost: float,
) -> tuple[float, float, float, float, str, int, int, float, int, int]:
    constrained: list[tuple[float, float, float, float, float, int, int]] = []
    all_rows: list[tuple[float, float, float, float, float, int, int]] = []

    for threshold in thresholds:
        t = float(threshold)
        precision, recall, f1, tp, fp = _metrics_with_counts(y_true, proba, t)
        utility = float(tp - (constrained_fp_cost * fp))
        all_rows.append((t, precision, recall, f1, utility, tp, fp))
        if precision >= precision_floor:
            constrained.append((t, precision, recall, f1, utility, tp, fp))

    reachable_count = int(len(constrained))
    candidate_count = int(len(all_rows))

    if constrained:
        # policy: among precision-safe thresholds, maximize utility, then F1.
        best = max(
            constrained, key=lambda row: (row[4], row[3], row[2], row[1], row[0])
        )
        return (
            best[0],
            best[1],
            best[2],
            best[3],
            "constrained_utility",
            reachable_count,
            candidate_count,
            best[4],
            best[5],
            best[6],
        )

    # deterministic fallback for sparse folds where floor is unreachable.
    best = max(all_rows, key=lambda row: (row[1], row[4], row[2], row[3], row[0]))
    return (
        best[0],
        best[1],
        best[2],
        best[3],
        "fallback_max_precision",
        reachable_count,
        candidate_count,
        best[4],
        best[5],
        best[6],
    )


def _parse_scale_pos_weight_grid(values: list[str]) -> list[tuple[str, float | None]]:
    parsed: list[tuple[str, float | None]] = []
    seen: set[tuple[str, float | None]] = set()

    for raw in values:
        token = str(raw).strip().lower()
        if token == "auto":
            key = ("auto", None)
            if key not in seen:
                parsed.append(key)
                seen.add(key)
            continue

        try:
            numeric = float(token)
        except ValueError as exc:
            raise ValueError(
                "--grid-scale-pos-weight accepts numeric values or 'auto'"
            ) from exc

        if numeric <= 0:
            raise ValueError("--grid-scale-pos-weight numeric values must be > 0")

        key = ("fixed", float(numeric))
        if key not in seen:
            parsed.append(key)
            seen.add(key)

    if not parsed:
        raise ValueError("--grid-scale-pos-weight produced an empty grid")

    return parsed


def _resolve_scale_pos_weight(
    mode: str, y_series: pd.Series, fixed_value: float | None
) -> float:
    if mode == "auto":
        return _compute_scale_pos_weight(y_series)

    if fixed_value is None:
        raise ValueError("fixed scale_pos_weight mode requires a numeric value")

    return float(fixed_value)


def _format_scale_pos_weight_label(mode: str, fixed_value: float | None) -> str:
    if mode == "auto":
        return "auto"
    if fixed_value is None:
        raise ValueError("fixed scale_pos_weight mode requires a numeric value")
    return f"{float(fixed_value):.6f}"


def _build_model(
    scale_pos_weight: float,
    max_delta_step: int,
    subsample: float,
    colsample_bytree: float,
) -> XGBClassifier:
    return XGBClassifier(
        n_estimators=400,
        learning_rate=0.05,
        max_depth=6,
        min_child_weight=1,
        max_delta_step=max_delta_step,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        objective="binary:logistic",
        eval_metric="aucpr",
        scale_pos_weight=scale_pos_weight,
        random_state=DEFAULT_RANDOM_STATE,
        n_jobs=-1,
    )


def _run_grouped_cv_for_config(
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    meta: pd.DataFrame,
    folds: int,
    threshold_grid: np.ndarray,
    baseline_threshold: float,
    precision_floor: float,
    constrained_fp_cost: float,
    max_delta_step: int,
    subsample: float,
    colsample_bytree: float,
    scale_pos_weight_mode: str,
    scale_pos_weight_fixed: float | None,
    calibration_policy: str,
    min_calibration_positives: int,
    collect_oof: bool,
    config_id: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    gkf = GroupKFold(n_splits=folds)
    fold_rows: list[dict[str, float | int | str]] = []
    oof_rows: list[dict[str, object]] = []

    for fold_idx, (train_idx, test_idx) in enumerate(
        gkf.split(X, y, groups=groups), start=1
    ):
        X_train = X.iloc[train_idx]
        X_test = X.iloc[test_idx]
        y_train = y.iloc[train_idx]
        y_test = y.iloc[test_idx]
        groups_train = groups.iloc[train_idx]

        train_groups = set(groups.iloc[train_idx].unique().tolist())
        test_groups = set(groups.iloc[test_idx].unique().tolist())
        overlap = train_groups.intersection(test_groups)
        if overlap:
            raise RuntimeError(
                f"group leakage detected in fold {fold_idx}: {sorted(overlap)}"
            )

        inner_splits = min(4, groups_train.nunique())
        if inner_splits < 2:
            raise RuntimeError(
                f"insufficient inner groups for calibration in fold {fold_idx}: {groups_train.nunique()}"
            )

        inner_gkf = GroupKFold(n_splits=inner_splits)
        # fit model on one inner block and reserve the other for honest calibration/threshold tuning.
        fit_rel_idx, calib_rel_idx = next(
            inner_gkf.split(X_train, y_train, groups=groups_train)
        )

        X_fit = X_train.iloc[fit_rel_idx]
        y_fit = y_train.iloc[fit_rel_idx]
        X_calib = X_train.iloc[calib_rel_idx]
        y_calib = y_train.iloc[calib_rel_idx]

        fold_spw = _resolve_scale_pos_weight(
            scale_pos_weight_mode, y_train, scale_pos_weight_fixed
        )
        calib_spw = _resolve_scale_pos_weight(
            scale_pos_weight_mode, y_fit, scale_pos_weight_fixed
        )

        calib_model = _build_model(
            scale_pos_weight=calib_spw,
            max_delta_step=max_delta_step,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
        )
        calib_model.fit(X_fit, y_fit)

        calib_raw_proba = calib_model.predict_proba(X_calib)[:, 1]
        calibrator, calibrator_label = _fit_calibrator(
            calib_raw_proba,
            y_calib,
            calibration_policy=calibration_policy,
            min_calibration_positives=min_calibration_positives,
        )
        calib_proba = _apply_calibrator(calibrator, calib_raw_proba)

        # lock threshold policy on inner calibration rows before touching outer test rows.
        unconstrained_threshold, _, _, _ = _find_best_f1_threshold_unconstrained(
            y_calib,
            calib_proba,
            threshold_grid,
        )
        (
            constrained_threshold,
            _,
            _,
            _,
            constrained_mode,
            reachable_thresholds,
            threshold_candidates,
            constrained_utility,
            constrained_tp,
            constrained_fp,
        ) = _find_best_f1_threshold_with_precision_floor(
            y_calib,
            calib_proba,
            threshold_grid,
            precision_floor,
            constrained_fp_cost,
        )

        model = _build_model(
            scale_pos_weight=fold_spw,
            max_delta_step=max_delta_step,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
        )
        model.fit(X_train, y_train)

        raw_proba = model.predict_proba(X_test)[:, 1]
        proba = _apply_calibrator(calibrator, raw_proba)

        pr_auc_raw = float(average_precision_score(y_test, raw_proba))
        pr_auc_calibrated = float(average_precision_score(y_test, proba))
        test_prevalence = float(np.mean(np.asarray(y_test, dtype=int)))
        pr_auc_lift_raw = float(pr_auc_raw - test_prevalence)
        pr_auc_lift_calibrated = float(pr_auc_calibrated - test_prevalence)
        brier = float(brier_score_loss(y_test, proba))

        baseline_precision, baseline_recall, baseline_f1 = _metrics_at_threshold(
            y_test, proba, baseline_threshold
        )
        unconstrained_precision, unconstrained_recall, unconstrained_f1 = (
            _metrics_at_threshold(
                y_test,
                proba,
                unconstrained_threshold,
            )
        )
        constrained_precision, constrained_recall, constrained_f1 = (
            _metrics_at_threshold(
                y_test,
                proba,
                constrained_threshold,
            )
        )

        row: dict[str, float | int | str] = {
            "fold": fold_idx,
            "test_rows": int(len(test_idx)),
            "test_positives": int((y_test == 1).sum()),
            "calibration_rows": int(len(calib_rel_idx)),
            "calibration_positives": int((y_calib == 1).sum()),
            "calibration_method_used": calibrator_label,
            "calibration_policy": calibration_policy,
            "scale_pos_weight": float(fold_spw),
            "scale_pos_weight_mode": scale_pos_weight_mode,
            "pr_auc": pr_auc_raw,
            "pr_auc_raw": pr_auc_raw,
            "pr_auc_calibrated": pr_auc_calibrated,
            "test_prevalence": test_prevalence,
            "pr_auc_lift_raw": pr_auc_lift_raw,
            "pr_auc_lift_calibrated": pr_auc_lift_calibrated,
            "baseline_precision": baseline_precision,
            "baseline_recall": baseline_recall,
            "baseline_f1": baseline_f1,
            "unconstrained_threshold": unconstrained_threshold,
            "unconstrained_precision": unconstrained_precision,
            "unconstrained_recall": unconstrained_recall,
            "unconstrained_f1": unconstrained_f1,
            "constrained_threshold": constrained_threshold,
            "constrained_precision": constrained_precision,
            "constrained_recall": constrained_recall,
            "constrained_f1": constrained_f1,
            "constrained_mode": constrained_mode,
            "precision_floor_reachable_thresholds": int(reachable_thresholds),
            "precision_floor_reachable_ratio": (
                float(reachable_thresholds / threshold_candidates)
                if threshold_candidates > 0
                else 0.0
            ),
            "constrained_margin_to_floor": float(
                constrained_precision - precision_floor
            ),
            "constrained_utility": float(constrained_utility),
            "constrained_tp": int(constrained_tp),
            "constrained_fp": int(constrained_fp),
            "brier": brier,
        }
        fold_rows.append(row)

        if collect_oof:
            meta_test = meta.iloc[test_idx]
            oof_rows.extend(
                {
                    "config_id": int(config_id),
                    "fold": int(fold_idx),
                    "race": str(race),
                    "driver": str(driver),
                    "lapNumber": int(lap_number) if pd.notna(lap_number) else pd.NA,
                    "target_y": int(target_value),
                    "raw_proba": float(raw_score),
                    "calibrated_proba": float(cal_score),
                    "baseline_threshold": float(baseline_threshold),
                    "unconstrained_threshold": float(unconstrained_threshold),
                    "constrained_threshold": float(constrained_threshold),
                    "baseline_pred": int(cal_score >= baseline_threshold),
                    "unconstrained_pred": int(cal_score >= unconstrained_threshold),
                    "constrained_pred": int(cal_score >= constrained_threshold),
                    "constrained_mode": str(constrained_mode),
                    "calibration_method_used": str(calibrator_label),
                }
                for race, driver, lap_number, target_value, raw_score, cal_score in zip(
                    meta_test["race"],
                    meta_test["driver"],
                    meta_test["lapNumber"],
                    y_test,
                    raw_proba,
                    proba,
                )
            )

        print(
            "fold {fold}: test_rows={test_rows}, pos={test_positives}, calib_rows={cal_rows}, "
            "calib_pos={cal_pos}, spw={spw:.4f}, "
            "pr_auc_raw={pr:.4f}, pr_auc_lift={lift:.4f}, brier={brier:.4f}, "
            "baseline(f1={bf1:.4f}, p={bp:.4f}, r={br:.4f}), "
            "unconstrained(t={uthr:.4f}, f1={uf1:.4f}, p={up:.4f}, r={ur:.4f}), "
            "constrained[{mode}](t={cthr:.4f}, f1={cf1:.4f}, p={cp:.4f}, r={cr:.4f}, reach={reach:.2f}, util={util:.2f}, cal={cal})".format(
                fold=fold_idx,
                test_rows=row["test_rows"],
                test_positives=row["test_positives"],
                cal_rows=row["calibration_rows"],
                cal_pos=row["calibration_positives"],
                spw=fold_spw,
                pr=pr_auc_raw,
                lift=pr_auc_lift_raw,
                brier=brier,
                bf1=baseline_f1,
                bp=baseline_precision,
                br=baseline_recall,
                uthr=unconstrained_threshold,
                uf1=unconstrained_f1,
                up=unconstrained_precision,
                ur=unconstrained_recall,
                mode=constrained_mode,
                cthr=constrained_threshold,
                cf1=constrained_f1,
                cp=constrained_precision,
                cr=constrained_recall,
                reach=(
                    (reachable_thresholds / threshold_candidates)
                    if threshold_candidates > 0
                    else 0.0
                ),
                util=constrained_utility,
                cal=calibrator_label,
            )
        )

    return pd.DataFrame(fold_rows), pd.DataFrame(oof_rows)


def _summarize_config(
    fold_df: pd.DataFrame,
    config_id: int,
    max_delta_step: int,
    subsample: float,
    colsample_bytree: float,
    scale_pos_weight_mode: str,
    scale_pos_weight_fixed: float | None,
) -> dict[str, float | int | str]:
    fallback_count = int(
        (fold_df["constrained_mode"] == "fallback_max_precision").sum()
    )
    total_folds = int(len(fold_df))
    fallback_rate = float(fallback_count / total_folds) if total_folds > 0 else 0.0
    reachable_folds = int((fold_df["precision_floor_reachable_thresholds"] > 0).sum())

    calibration_mix = fold_df["calibration_method_used"].value_counts().to_dict()
    calibration_mix_label = ",".join(
        f"{key}:{value}" for key, value in calibration_mix.items()
    )

    spw_label = _format_scale_pos_weight_label(
        scale_pos_weight_mode, scale_pos_weight_fixed
    )

    return {
        "config_id": int(config_id),
        "max_delta_step": int(max_delta_step),
        "subsample": float(subsample),
        "colsample_bytree": float(colsample_bytree),
        "scale_pos_weight_mode": scale_pos_weight_mode,
        "scale_pos_weight_label": spw_label,
        "scale_pos_weight_fixed": (
            float(scale_pos_weight_fixed)
            if scale_pos_weight_fixed is not None
            else np.nan
        ),
        "mean_effective_scale_pos_weight": float(fold_df["scale_pos_weight"].mean()),
        "mean_pr_auc": float(fold_df["pr_auc"].mean()),
        "mean_pr_auc_raw": float(fold_df["pr_auc_raw"].mean()),
        "mean_pr_auc_calibrated": float(fold_df["pr_auc_calibrated"].mean()),
        "mean_test_prevalence": float(fold_df["test_prevalence"].mean()),
        "mean_pr_auc_lift_raw": float(fold_df["pr_auc_lift_raw"].mean()),
        "mean_pr_auc_lift_calibrated": float(fold_df["pr_auc_lift_calibrated"].mean()),
        "std_pr_auc": float(fold_df["pr_auc"].std(ddof=0)),
        "mean_baseline_f1": float(fold_df["baseline_f1"].mean()),
        "mean_baseline_precision": float(fold_df["baseline_precision"].mean()),
        "mean_baseline_recall": float(fold_df["baseline_recall"].mean()),
        "mean_unconstrained_f1": float(fold_df["unconstrained_f1"].mean()),
        "mean_unconstrained_precision": float(
            fold_df["unconstrained_precision"].mean()
        ),
        "mean_unconstrained_recall": float(fold_df["unconstrained_recall"].mean()),
        "mean_unconstrained_threshold": float(
            fold_df["unconstrained_threshold"].mean()
        ),
        "mean_constrained_f1": float(fold_df["constrained_f1"].mean()),
        "mean_constrained_precision": float(fold_df["constrained_precision"].mean()),
        "mean_constrained_recall": float(fold_df["constrained_recall"].mean()),
        "mean_constrained_threshold": float(fold_df["constrained_threshold"].mean()),
        "mean_constrained_margin_to_floor": float(
            fold_df["constrained_margin_to_floor"].mean()
        ),
        "mean_precision_floor_reachable_ratio": float(
            fold_df["precision_floor_reachable_ratio"].mean()
        ),
        "precision_floor_reachable_folds": reachable_folds,
        "mean_constrained_utility": float(fold_df["constrained_utility"].mean()),
        "calibration_mix": calibration_mix_label,
        "fallback_folds": fallback_count,
        "total_folds": total_folds,
        "fallback_rate": fallback_rate,
        "mean_brier": float(fold_df["brier"].mean()),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="train pit strategy model with grouped cross validation"
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help="path to ML training dataset (csv or parquet)",
    )
    parser.add_argument(
        "--folds", type=int, default=DEFAULT_FOLDS, help="number of group folds"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_PROBA_THRESHOLD,
        help="baseline probability threshold for comparison",
    )
    parser.add_argument(
        "--sweep-min",
        type=float,
        default=DEFAULT_SWEEP_MIN,
        help="minimum tuned threshold",
    )
    parser.add_argument(
        "--sweep-max",
        type=float,
        default=DEFAULT_SWEEP_MAX,
        help="maximum tuned threshold",
    )
    parser.add_argument(
        "--sweep-points",
        type=int,
        default=DEFAULT_SWEEP_POINTS,
        help="number of thresholds in sweep grid",
    )
    parser.add_argument(
        "--precision-floor",
        type=float,
        default=DEFAULT_PRECISION_FLOOR,
        help="minimum precision required during constrained threshold search",
    )
    parser.add_argument(
        "--constrained-fp-cost",
        type=float,
        default=DEFAULT_CONSTRAINED_FP_COST,
        help="false-positive penalty in constrained threshold utility",
    )
    parser.add_argument(
        "--calibration-policy",
        choices=["auto", "isotonic", "sigmoid"],
        default=DEFAULT_CALIBRATION_POLICY,
        help="calibration strategy for per-fold probability correction",
    )
    parser.add_argument(
        "--min-calibration-positives",
        type=int,
        default=DEFAULT_MIN_CALIBRATION_POSITIVES,
        help="minimum positives required before auto policy attempts isotonic calibration",
    )
    parser.add_argument(
        "--grid-max-delta-step",
        type=int,
        nargs="+",
        default=list(DEFAULT_GRID_MAX_DELTA_STEP),
        help="ablation grid for max_delta_step",
    )
    parser.add_argument(
        "--grid-subsample",
        type=float,
        nargs="+",
        default=list(DEFAULT_GRID_SUBSAMPLE),
        help="ablation grid for subsample",
    )
    parser.add_argument(
        "--grid-colsample-bytree",
        type=float,
        nargs="+",
        default=list(DEFAULT_GRID_COLSAMPLE_BYTREE),
        help="ablation grid for colsample_bytree",
    )
    parser.add_argument(
        "--grid-scale-pos-weight",
        nargs="+",
        default=list(DEFAULT_GRID_SCALE_POS_WEIGHT),
        help="ablation grid for scale_pos_weight (use 'auto' and/or numeric values)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_LEADERBOARD_TOP_K,
        help="number of top ablation rows printed",
    )
    parser.add_argument(
        "--leaderboard-output",
        default=DEFAULT_LEADERBOARD_OUTPUT,
        help="optional csv output path for full ablation leaderboard",
    )
    parser.add_argument(
        "--oof-output",
        default=DEFAULT_OOF_OUTPUT,
        help="optional csv output path for winner out-of-fold decisions",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)

    if args.folds < 2:
        raise ValueError("--folds must be at least 2")
    if not (0.0 < args.threshold < 1.0):
        raise ValueError("--threshold must be between 0 and 1")
    if not (0.0 < args.sweep_min < args.sweep_max < 1.0):
        raise ValueError("--sweep-min and --sweep-max must satisfy 0 < min < max < 1")
    if args.sweep_points < 2:
        raise ValueError("--sweep-points must be at least 2")
    if not (0.0 < args.precision_floor < 1.0):
        raise ValueError("--precision-floor must be between 0 and 1")
    if args.constrained_fp_cost < 0.0:
        raise ValueError("--constrained-fp-cost must be >= 0")
    if args.min_calibration_positives < 1:
        raise ValueError("--min-calibration-positives must be at least 1")
    if args.top_k < 1:
        raise ValueError("--top-k must be at least 1")

    threshold_grid = np.linspace(args.sweep_min, args.sweep_max, args.sweep_points)

    grid_max_delta_steps = sorted(set(args.grid_max_delta_step))
    if any(value < 0 for value in grid_max_delta_steps):
        raise ValueError("--grid-max-delta-step values must be >= 0")

    grid_subsamples = sorted(set(args.grid_subsample))
    if any(value <= 0.0 or value > 1.0 for value in grid_subsamples):
        raise ValueError("--grid-subsample values must satisfy 0 < value <= 1")

    grid_colsamples = sorted(set(args.grid_colsample_bytree))
    if any(value <= 0.0 or value > 1.0 for value in grid_colsamples):
        raise ValueError("--grid-colsample-bytree values must satisfy 0 < value <= 1")

    grid_scale_pos_weight = _parse_scale_pos_weight_grid(args.grid_scale_pos_weight)

    config_grid = list(
        product(
            grid_max_delta_steps,
            grid_subsamples,
            grid_colsamples,
            grid_scale_pos_weight,
        )
    )
    if not config_grid:
        raise RuntimeError("ablation grid is empty")

    df = _load_dataset(dataset_path)
    X, y, groups, meta = _prepare_matrix(df)

    if groups.nunique() < args.folds:
        raise ValueError(
            f"--folds={args.folds} exceeds available unique races ({groups.nunique()}) in dataset"
        )

    positives = int((y == 1).sum())
    negatives = int((y == 0).sum())
    global_spw = _compute_scale_pos_weight(y)

    spw_labels = [
        _format_scale_pos_weight_label(mode, value)
        for mode, value in grid_scale_pos_weight
    ]

    print("=== PIT STRATEGY TRAINING SUMMARY ===")
    print(f"dataset                 : {dataset_path}")
    print(f"rows                    : {len(df)}")
    print(f"usable rows             : {len(y)}")
    print(f"feature columns         : {X.shape[1]}")
    print(f"groups (races)          : {groups.nunique()}")
    print(f"class y=1               : {positives}")
    print(f"class y=0               : {negatives}")
    print(f"global scale_pos_weight : {global_spw:.6f}")
    print(f"baseline threshold      : {args.threshold:.4f}")
    print(
        f"sweep range             : [{args.sweep_min:.2f}, {args.sweep_max:.2f}] with {args.sweep_points} points"
    )
    print(f"precision floor         : {args.precision_floor:.2f}")
    print(f"constrained fp cost     : {args.constrained_fp_cost:.2f}")
    print(f"calibration policy      : {args.calibration_policy}")
    print(f"min calibration pos     : {args.min_calibration_positives}")
    print(f"grid max_delta_step     : {grid_max_delta_steps}")
    print(f"grid subsample          : {grid_subsamples}")
    print(f"grid colsample_bytree   : {grid_colsamples}")
    print(f"grid scale_pos_weight   : {spw_labels}")
    print(f"total configurations    : {len(config_grid)}")

    leaderboard_rows: list[dict[str, float | int | str]] = []
    collect_oof = bool(args.oof_output)
    oof_frames: list[pd.DataFrame] = []

    for config_id, (max_delta_step, subsample, colsample_bytree, spw_cfg) in enumerate(
        config_grid, start=1
    ):
        spw_mode, spw_fixed = spw_cfg
        spw_label = _format_scale_pos_weight_label(spw_mode, spw_fixed)

        print(f"\n=== CONFIG {config_id}/{len(config_grid)} ===")
        print(f"max_delta_step       : {max_delta_step}")
        print(f"subsample            : {subsample:.4f}")
        print(f"colsample_bytree     : {colsample_bytree:.4f}")
        print(f"scale_pos_weight     : {spw_label}")

        fold_df, oof_df = _run_grouped_cv_for_config(
            X=X,
            y=y,
            groups=groups,
            meta=meta,
            folds=args.folds,
            threshold_grid=threshold_grid,
            baseline_threshold=args.threshold,
            precision_floor=args.precision_floor,
            constrained_fp_cost=args.constrained_fp_cost,
            max_delta_step=max_delta_step,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
            scale_pos_weight_mode=spw_mode,
            scale_pos_weight_fixed=spw_fixed,
            calibration_policy=args.calibration_policy,
            min_calibration_positives=args.min_calibration_positives,
            collect_oof=collect_oof,
            config_id=config_id,
        )

        if collect_oof and not oof_df.empty:
            oof_frames.append(oof_df)

        summary = _summarize_config(
            fold_df=fold_df,
            config_id=config_id,
            max_delta_step=max_delta_step,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
            scale_pos_weight_mode=spw_mode,
            scale_pos_weight_fixed=spw_fixed,
        )
        leaderboard_rows.append(summary)

        print(
            "config mean: pr_auc={pr:.6f}, pr_lift={lift:.6f}, constrained_f1={cf1:.6f}, "
            "reach={reach:.3f}, constrained_fallback={fb}/{tot}, brier={brier:.6f}, cal_mix={cal}".format(
                pr=summary["mean_pr_auc"],
                lift=summary["mean_pr_auc_lift_raw"],
                cf1=summary["mean_constrained_f1"],
                reach=summary["mean_precision_floor_reachable_ratio"],
                fb=summary["fallback_folds"],
                tot=summary["total_folds"],
                brier=summary["mean_brier"],
                cal=summary["calibration_mix"],
            )
        )

    leaderboard_df = pd.DataFrame(leaderboard_rows)
    leaderboard_df.sort_values(
        by=[
            "mean_pr_auc",
            "mean_pr_auc_lift_raw",
            "mean_constrained_f1",
            "mean_unconstrained_f1",
            "mean_constrained_precision",
            "mean_brier",
            "fallback_rate",
        ],
        ascending=[False, False, False, False, False, True, True],
        inplace=True,
    )
    leaderboard_df.reset_index(drop=True, inplace=True)

    top_k = min(args.top_k, len(leaderboard_df))
    display_cols = [
        "config_id",
        "max_delta_step",
        "subsample",
        "colsample_bytree",
        "scale_pos_weight_label",
        "mean_pr_auc",
        "mean_pr_auc_lift_raw",
        "mean_constrained_f1",
        "mean_unconstrained_f1",
        "mean_constrained_precision",
        "mean_precision_floor_reachable_ratio",
        "mean_constrained_margin_to_floor",
        "fallback_folds",
        "total_folds",
        "calibration_mix",
        "mean_brier",
    ]

    print(f"\n=== ABLATION LEADERBOARD (TOP {top_k}) ===")
    print(leaderboard_df.loc[: top_k - 1, display_cols].to_string(index=False))

    winner = leaderboard_df.iloc[0]
    print("\n=== SELECTED WINNER ===")
    print(f"config_id            : {int(winner['config_id'])}")
    print(f"max_delta_step       : {int(winner['max_delta_step'])}")
    print(f"subsample            : {float(winner['subsample']):.4f}")
    print(f"colsample_bytree     : {float(winner['colsample_bytree']):.4f}")
    print(f"scale_pos_weight     : {winner['scale_pos_weight_label']}")
    print(f"mean_pr_auc          : {float(winner['mean_pr_auc']):.6f}")
    print(f"mean_pr_auc_lift     : {float(winner['mean_pr_auc_lift_raw']):.6f}")
    print(f"mean_constrained_f1  : {float(winner['mean_constrained_f1']):.6f}")
    print(f"mean_unconstrained_f1: {float(winner['mean_unconstrained_f1']):.6f}")
    print(
        f"mean_reachability    : {float(winner['mean_precision_floor_reachable_ratio']):.6f}"
    )
    print(
        f"mean_margin_to_floor : {float(winner['mean_constrained_margin_to_floor']):.6f}"
    )
    print(f"mean_constrained_util: {float(winner['mean_constrained_utility']):.6f}")
    print(f"calibration mix      : {winner['calibration_mix']}")
    print(
        "constrained_fallback : {fallbacks}/{total}".format(
            fallbacks=int(winner["fallback_folds"]),
            total=int(winner["total_folds"]),
        )
    )
    print(f"mean_brier           : {float(winner['mean_brier']):.6f}")

    if args.leaderboard_output:
        output_path = Path(args.leaderboard_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        leaderboard_df.to_csv(output_path, index=False)
        print(f"leaderboard csv      : {output_path}")

    if collect_oof and oof_frames:
        winner_config_id = int(winner["config_id"])
        oof_df = pd.concat(oof_frames, ignore_index=True)
        oof_df = oof_df[oof_df["config_id"] == winner_config_id].copy()
        oof_output_path = Path(args.oof_output)
        oof_output_path.parent.mkdir(parents=True, exist_ok=True)
        oof_df.to_csv(oof_output_path, index=False)
        print(f"winner oof csv       : {oof_output_path}")
        print(f"winner oof rows      : {len(oof_df)}")


if __name__ == "__main__":
    main()
