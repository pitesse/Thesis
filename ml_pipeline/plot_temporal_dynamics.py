"""Generate true local temporal dynamics for Batch vs Streaming ML."""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import warnings

from lib.moa_predictions import decode_moa_predictions
from lib.model_training_cv import TARGET_COLUMN

warnings.filterwarnings('ignore')

def calculate_rolling_metrics(y_true, y_pred, window_size=3000):
    """Calcola Accuracy e Kappa su finestra mobile REALE."""
    y_true_s = pd.Series(y_true)
    y_pred_s = pd.Series(y_pred)
    
    acc = y_true_s.eq(y_pred_s).rolling(window=window_size, min_periods=500).mean()
    
    p_o = acc
    p_true_1 = y_true_s.rolling(window=window_size, min_periods=500).mean()
    p_pred_1 = y_pred_s.rolling(window=window_size, min_periods=500).mean()
    p_e = (p_true_1 * p_pred_1) + ((1 - p_true_1) * (1 - p_pred_1))
    
    kappa = (p_o - p_e) / (1 - p_e + 1e-9)
    return acc.fillna(method='bfill').values, kappa.fillna(0).values

def main():
    reports_dir = Path("data_lake/reports")
    dataset_path = Path("data_lake/ml_training_dataset_2022_2025_merged.parquet")
    moa_pred_path = reports_dir / "moa_arf_predictions_2022_2025_merged.pred"
    ml_oof_path = reports_dir / "ml_oof_winner_2022_2025_merged.csv"
    
    # Ground Truth
    df_true = pd.read_parquet(dataset_path)
    y_true_full = df_true[TARGET_COLUMN].astype(int).values
    total_laps = len(y_true_full)
    
    # predizioni SML giro per giro
    y_moa_pred_series, _ = decode_moa_predictions(moa_pred_path, df_true[TARGET_COLUMN].astype(int))
    y_moa_pred = y_moa_pred_series.fillna(0).astype(int).values
    
    # metriche reali per SML
    print("Calcolo finestra mobile SML in corso...")
    sml_acc, sml_kappa = calculate_rolling_metrics(y_true_full, y_moa_pred)
    instances_sml = np.arange(total_laps)
    
    # predizioni Batch ML
    df_ml = pd.read_csv(ml_oof_path)
    y_ml_true = df_ml['target_y'].values
    y_ml_pred = df_ml['constrained_pred'].values
    
    print("Calcolo finestra mobile Batch ML in corso...")
    batch_acc, batch_kappa = calculate_rolling_metrics(y_ml_true, y_ml_pred)
    
    # Il Batch ML salta le prime gare per il training, quindi si allinea temporalmente alla fine.
    start_idx = total_laps - len(batch_acc)
    instances_batch = np.arange(start_idx, total_laps)

    # accuracy over time plot
    plt.figure(figsize=(14, 6))
    plt.plot(instances_sml, sml_acc, color='#2ca02c', linewidth=1.5, alpha=0.9, label='Streaming ML (Real Local Window)')
    plt.plot(instances_batch, batch_acc, color='#1f77b4', linewidth=1.5, alpha=0.8, label='Batch ML (Real Local Window)')

    plt.title(' Local Accuracy over Time (3000-lap window)', fontsize=14, pad=15)
    plt.xlabel('Processed Laps (Timeline)', fontsize=12)
    plt.ylabel('Local Accuracy', fontsize=12)
    plt.ylim(0.70, 1.01)
    plt.legend(loc='lower right')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(reports_dir / 'paper_fig0_accuracy_over_time_real.pdf', dpi=300)
    plt.close()

    # kappa over time plot
    plt.figure(figsize=(14, 6))
    plt.plot(instances_sml, sml_kappa, color='#ff7f0e', linewidth=1.5, alpha=0.9, label='Streaming ML (Real Local Window)')
    plt.plot(instances_batch, batch_kappa, color='#9467bd', linewidth=1.5, alpha=0.8, label='Batch ML (Real Local Window)')

    plt.title(' Local Kappa Statistic over Time (Concept Drift Varianza)', fontsize=14, pad=15)
    plt.xlabel('Processed Laps (Timeline)', fontsize=12)
    plt.ylabel('Local Kappa', fontsize=12)
    plt.ylim(-0.1, 0.6)
    plt.legend(loc='upper right')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(reports_dir / 'paper_fig1_kappa_over_time_real.pdf', dpi=300)
    plt.close()

    print("generati con successo in data_lake/reports/")

if __name__ == "__main__":
    main()