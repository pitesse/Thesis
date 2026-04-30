"""Shared surrogate-model sweep and selection utilities for MOA explainability."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

SURROGATE_BASELINE_ID = "rf_100_d10"


@dataclass(frozen=True)
class SurrogateSpec:
    model_id: str
    family: str
    supports_tree_shap: bool
    build: Callable[[int], object]


def _candidate_specs() -> list[SurrogateSpec]:
    return [
        SurrogateSpec(
            model_id="rf_100_d10",
            family="RandomForestClassifier",
            supports_tree_shap=True,
            build=lambda seed: RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=seed,
                n_jobs=-1,
                class_weight="balanced",
            ),
        ),
        SurrogateSpec(
            model_id="rf_300_d12",
            family="RandomForestClassifier",
            supports_tree_shap=True,
            build=lambda seed: RandomForestClassifier(
                n_estimators=300,
                max_depth=12,
                random_state=seed,
                n_jobs=-1,
                class_weight="balanced",
            ),
        ),
        SurrogateSpec(
            model_id="extra_300_d12",
            family="ExtraTreesClassifier",
            supports_tree_shap=True,
            build=lambda seed: ExtraTreesClassifier(
                n_estimators=300,
                max_depth=12,
                random_state=seed,
                n_jobs=-1,
                class_weight="balanced",
            ),
        ),
        SurrogateSpec(
            model_id="hgb_400_d8",
            family="HistGradientBoostingClassifier",
            supports_tree_shap=False,
            build=lambda seed: HistGradientBoostingClassifier(
                max_iter=400,
                max_depth=8,
                random_state=seed,
            ),
        ),
        SurrogateSpec(
            model_id="logreg_balanced",
            family="LogisticRegression+Imputer+Scaler",
            supports_tree_shap=False,
            build=lambda seed: Pipeline(
                steps=[
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler()),
                    (
                        "classifier",
                        LogisticRegression(
                            max_iter=2000,
                            class_weight="balanced",
                            random_state=seed,
                        ),
                    ),
                ]
            ),
        ),
    ]


def evaluate_and_select_surrogate(
    X_train,
    X_test,
    y_train,
    y_test,
    *,
    seed: int,
    min_f1_gain: float,
) -> tuple[pd.DataFrame, object, dict[str, object]]:
    """Train fixed surrogate candidates and return a selected model.

    Selection rule:
    1) Rank candidates by F1, balanced accuracy, accuracy.
    2) If top candidate improves F1 by at least ``min_f1_gain`` over baseline,
       keep top candidate; otherwise keep baseline for stability.
    """

    if min_f1_gain < 0:
        raise ValueError("min_f1_gain must be >= 0")

    candidates = _candidate_specs()
    fitted_models: dict[str, object] = {}
    rows: list[dict[str, object]] = []

    for spec in candidates:
        model = spec.build(seed)
        model.fit(X_train, y_train)
        y_hat = model.predict(X_test)

        fitted_models[spec.model_id] = model
        rows.append(
            {
                "model_id": spec.model_id,
                "family": spec.family,
                "supports_tree_shap": int(spec.supports_tree_shap),
                "fidelity_accuracy": float(accuracy_score(y_test, y_hat)),
                "fidelity_f1": float(f1_score(y_test, y_hat, zero_division=0)),
                "fidelity_precision": float(precision_score(y_test, y_hat, zero_division=0)),
                "fidelity_recall": float(recall_score(y_test, y_hat, zero_division=0)),
                "fidelity_balanced_accuracy": float(balanced_accuracy_score(y_test, y_hat)),
            }
        )

    sweep_df = pd.DataFrame(rows)
    sweep_df = sweep_df.sort_values(
        by=["fidelity_f1", "fidelity_balanced_accuracy", "fidelity_accuracy", "model_id"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    sweep_df.insert(0, "rank", range(1, len(sweep_df) + 1))

    if SURROGATE_BASELINE_ID not in set(sweep_df["model_id"].astype(str)):
        raise ValueError(f"baseline surrogate id not present in sweep: {SURROGATE_BASELINE_ID}")

    top_row = sweep_df.iloc[0]
    baseline_row = sweep_df[sweep_df["model_id"] == SURROGATE_BASELINE_ID].iloc[0]

    top_f1 = float(top_row["fidelity_f1"])
    baseline_f1 = float(baseline_row["fidelity_f1"])

    if top_f1 - baseline_f1 >= float(min_f1_gain):
        selected_id = str(top_row["model_id"])
        selection_reason = (
            f"selected top-ranked candidate by F1 (gain={top_f1 - baseline_f1:.6f} >= min_f1_gain={float(min_f1_gain):.6f})"
        )
    else:
        selected_id = SURROGATE_BASELINE_ID
        selection_reason = (
            f"kept baseline for stability because best F1 gain ({top_f1 - baseline_f1:.6f}) < min_f1_gain={float(min_f1_gain):.6f}"
        )

    sweep_df["selected"] = (sweep_df["model_id"] == selected_id).astype(int)
    selected_row = sweep_df[sweep_df["model_id"] == selected_id].iloc[0]

    selection_info = {
        "baseline_model_id": SURROGATE_BASELINE_ID,
        "selected_model_id": selected_id,
        "selected_family": str(selected_row["family"]),
        "selected_supports_tree_shap": bool(int(selected_row["supports_tree_shap"])),
        "selection_reason": selection_reason,
        "min_f1_gain": float(min_f1_gain),
        "best_f1": top_f1,
        "baseline_f1": baseline_f1,
    }

    selected_model = fitted_models[selected_id]
    return sweep_df, selected_model, selection_info
