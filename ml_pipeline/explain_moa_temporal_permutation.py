"""Temporal permutation explainability for MOA predictions.

This script adds a second explainability method to complement surrogate SHAP:
- fit a surrogate model to MOA decoded predictions,
- compute global permutation importance,
- compute windowed permutation importance across the timeline,
- compare top features with existing surrogate-SHAP top features.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split

from lib.moa_predictions import decode_moa_predictions
from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    default_report_csv,
    normalize_years,
    reports_dir,
)

TARGET_COLUMN = "target_y"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute temporal permutation importance for MOA predictions"
    )
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE)
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS))
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG)

    parser.add_argument("--moa-dataset-csv", default="")
    parser.add_argument("--moa-predictions", default="")
    parser.add_argument("--moa-shap-summary-json", default="")

    parser.add_argument("--reports-dir", default="")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--window-count", type=int, default=12)
    parser.add_argument("--window-min-rows", type=int, default=2000)
    parser.add_argument("--top-features", type=int, default=10)
    parser.add_argument("--perm-repeats", type=int, default=5)
    return parser.parse_args()


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Path, Path, Path]:
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)

    dataset_csv = (
        Path(args.moa_dataset_csv)
        if args.moa_dataset_csv
        else default_report_csv(data_lake, "moa_dataset", years, args.season_tag)
    )
    pred_path = (
        Path(args.moa_predictions)
        if args.moa_predictions
        else default_report_csv(data_lake, "moa_arf_predictions", years, args.season_tag).with_suffix(".pred")
    )
    output_dir = Path(args.reports_dir) if args.reports_dir else reports_dir(data_lake)
    shap_summary_json = (
        Path(args.moa_shap_summary_json)
        if args.moa_shap_summary_json
        else output_dir / "moa_shap_proxy_summary.json"
    )

    for path in [dataset_csv, pred_path]:
        if not path.exists():
            raise FileNotFoundError(f"required input not found: {path}")
    return dataset_csv, pred_path, output_dir, shap_summary_json


def _extract_shap_top_features(shap_summary_json: Path) -> list[str]:
    if not shap_summary_json.exists():
        return []

    payload = json.loads(shap_summary_json.read_text(encoding="utf-8"))
    artifacts = payload.get("artifacts", [])
    top_features: list[str] = []
    for item in artifacts:
        token = str(item)
        marker = "moa_shap_proxy_dependence_"
        if marker in token:
            feature = token.split(marker, maxsplit=1)[1].rsplit(".", maxsplit=1)[0]
            top_features.append(feature)
    return top_features


def _load_aligned_dataset(dataset_csv: Path, pred_path: Path) -> tuple[pd.DataFrame, pd.Series, dict[str, object]]:
    frame = pd.read_csv(dataset_csv)
    if TARGET_COLUMN not in frame.columns:
        raise ValueError(f"target column not found in dataset: {TARGET_COLUMN}")

    y_true = pd.to_numeric(frame[TARGET_COLUMN], errors="coerce").fillna(0).astype(int)
    y_moa_pred, diagnostics = decode_moa_predictions(
        pred_path=pred_path,
        y_true=y_true,
        min_mapping_purity=0.99,
    )

    n = min(len(frame), len(y_moa_pred))
    frame = frame.iloc[:n].reset_index(drop=True)
    y_moa_pred = y_moa_pred.iloc[:n].reset_index(drop=True)

    known_mask = y_moa_pred.notna()
    frame = frame.loc[known_mask].reset_index(drop=True)
    y = y_moa_pred.loc[known_mask].astype(int).reset_index(drop=True)

    if y.nunique() < 2:
        raise ValueError("decoded MOA predictions contain one class only")

    diagnostics["rows_used_after_known_filter"] = int(len(frame))
    return frame, y, diagnostics


def _window_slices(total_rows: int, window_count: int, window_min_rows: int) -> list[tuple[int, int, str]]:
    if total_rows < window_min_rows:
        return [(0, total_rows, "full")]

    raw_edges = np.linspace(0, total_rows, num=window_count + 1, dtype=int)
    windows: list[tuple[int, int, str]] = []
    for i in range(window_count):
        start = int(raw_edges[i])
        end = int(raw_edges[i + 1])
        if end - start < window_min_rows:
            continue
        windows.append((start, end, f"w{i+1:02d}"))

    if not windows:
        windows.append((0, total_rows, "full"))
    return windows


def _plot_heatmap(window_df: pd.DataFrame, top_features: list[str], output_path: Path) -> None:
    if window_df.empty or not top_features:
        return

    pivot = (
        window_df[window_df["feature"].isin(top_features)]
        .pivot(index="feature", columns="window_id", values="importance_drop_f1")
        .reindex(index=top_features)
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    im = ax.imshow(pivot.to_numpy(dtype=float), aspect="auto", cmap="viridis")
    ax.set_title("MOA Temporal Permutation Importance (F1 drop)", fontsize=14, pad=12)
    ax.set_xlabel("Temporal window", fontsize=12)
    ax.set_ylabel("Feature", fontsize=12)
    ax.set_xticks(np.arange(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns.tolist(), rotation=45, ha="right")
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels(pivot.index.tolist())
    fig.colorbar(im, ax=ax, label="Permutation importance (F1 drop)")
    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)


def main() -> None:
    args = parse_args()

    if args.window_count < 1:
        raise ValueError("--window-count must be >= 1")
    if args.window_min_rows < 100:
        raise ValueError("--window-min-rows must be >= 100")
    if args.top_features < 1:
        raise ValueError("--top-features must be >= 1")
    if args.perm_repeats < 2:
        raise ValueError("--perm-repeats must be >= 2")

    dataset_csv, pred_path, output_dir, shap_summary_json = _resolve_paths(args)
    output_dir.mkdir(parents=True, exist_ok=True)

    frame, y, diagnostics = _load_aligned_dataset(dataset_csv, pred_path)

    X = frame.drop(columns=[TARGET_COLUMN]).astype(float)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=args.seed,
        stratify=y,
    )

    surrogate = RandomForestClassifier(
        n_estimators=250,
        max_depth=12,
        random_state=args.seed,
        n_jobs=-1,
        class_weight="balanced",
    )
    surrogate.fit(X_train, y_train)

    y_hat = surrogate.predict(X_test)
    fidelity_accuracy = float(accuracy_score(y_test, y_hat))
    fidelity_f1 = float(f1_score(y_test, y_hat, zero_division=0))

    # Global permutation importance on held-out rows.
    global_perm = permutation_importance(
        surrogate,
        X_test,
        y_test,
        n_repeats=args.perm_repeats,
        random_state=args.seed,
        scoring="f1",
        n_jobs=1,
    )

    global_df = pd.DataFrame(
        {
            "feature": X.columns,
            "importance_drop_f1": global_perm.importances_mean,
            "importance_std": global_perm.importances_std,
        }
    ).sort_values("importance_drop_f1", ascending=False)
    global_df.reset_index(drop=True, inplace=True)

    top_features = global_df.head(args.top_features)["feature"].tolist()

    # Temporal permutation importance across chronological windows.
    windows = _window_slices(len(X), args.window_count, args.window_min_rows)
    window_rows: list[dict[str, object]] = []

    for start, end, window_id in windows:
        X_w = X.iloc[start:end]
        y_w = y.iloc[start:end]

        if y_w.nunique() < 2:
            continue

        perm_w = permutation_importance(
            surrogate,
            X_w,
            y_w,
            n_repeats=args.perm_repeats,
            random_state=args.seed,
            scoring="f1",
            n_jobs=1,
        )

        for feature, imp_mean, imp_std in zip(
            X.columns,
            perm_w.importances_mean,
            perm_w.importances_std,
            strict=True,
        ):
            window_rows.append(
                {
                    "window_id": window_id,
                    "start_idx": int(start),
                    "end_idx": int(end),
                    "rows": int(end - start),
                    "feature": feature,
                    "importance_drop_f1": float(imp_mean),
                    "importance_std": float(imp_std),
                }
            )

    window_df = pd.DataFrame(window_rows)

    shap_top_features = _extract_shap_top_features(shap_summary_json)
    if shap_top_features:
        perm_rank = {feature: idx + 1 for idx, feature in enumerate(global_df["feature"].tolist())}
        agreement_rows = []
        for feature in shap_top_features:
            agreement_rows.append(
                {
                    "feature": feature,
                    "in_permutation_ranking": int(feature in perm_rank),
                    "permutation_rank": int(perm_rank[feature]) if feature in perm_rank else np.nan,
                }
            )
        agreement_df = pd.DataFrame(agreement_rows)
    else:
        agreement_df = pd.DataFrame(
            [{"feature": "N/A", "in_permutation_ranking": np.nan, "permutation_rank": np.nan}]
        )

    summary_df = pd.DataFrame(
        [
            {
                "dataset_csv": str(dataset_csv),
                "predictions_file": str(pred_path),
                "rows_used": int(len(X)),
                "decoded_unknown_prediction_rows": int(diagnostics.get("unknown_prediction_rows", 0)),
                "known_positive_rate": float(diagnostics.get("known_positive_rate", np.nan)),
                "fidelity_accuracy": fidelity_accuracy,
                "fidelity_f1": fidelity_f1,
                "window_count_used": int(window_df["window_id"].nunique()) if not window_df.empty else 0,
                "top_features": ";".join(top_features),
                "notes": "Permutation importance computed on surrogate predictions of MOA-decoded labels",
            }
        ]
    )

    summary_csv = output_dir / "moa_temporal_permutation_summary.csv"
    global_csv = output_dir / "moa_temporal_permutation_global.csv"
    windows_csv = output_dir / "moa_temporal_permutation_by_window.csv"
    agreement_csv = output_dir / "moa_temporal_permutation_shap_agreement.csv"
    heatmap_pdf = output_dir / "moa_temporal_permutation_heatmap.pdf"

    summary_df.to_csv(summary_csv, index=False)
    global_df.to_csv(global_csv, index=False)
    window_df.to_csv(windows_csv, index=False)
    agreement_df.to_csv(agreement_csv, index=False)
    _plot_heatmap(window_df, top_features, heatmap_pdf)

    payload = {
        "summary": summary_df.iloc[0].to_dict(),
        "decode_diagnostics": diagnostics,
        "shap_top_features": shap_top_features,
        "artifacts": [
            str(summary_csv),
            str(global_csv),
            str(windows_csv),
            str(agreement_csv),
            str(heatmap_pdf),
        ],
    }
    summary_json = output_dir / "moa_temporal_permutation_summary.json"
    summary_json.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    print("=== MOA TEMPORAL PERMUTATION SUMMARY ===")
    print(f"rows used               : {len(X)}")
    print(f"fidelity accuracy       : {fidelity_accuracy:.6f}")
    print(f"fidelity f1             : {fidelity_f1:.6f}")
    print(f"windows used            : {summary_df.iloc[0]['window_count_used']}")
    print(f"top permutation features: {', '.join(top_features)}")
    print(f"summary csv             : {summary_csv}")
    print(f"summary json            : {summary_json}")


if __name__ == "__main__":
    main()
