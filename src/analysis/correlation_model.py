from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from analysis.utils import load_all_close_price_via_api, get_all_tickers_via_api, CORR_MATRIX_PATH


def compute_log_returns(price_df: pd.DataFrame) -> pd.DataFrame:
    """Compute log returns."""
    return np.log(price_df / price_df.shift(1)).dropna()


def compute_correlation_matrix(log_returns: pd.DataFrame) -> pd.DataFrame:
    """Correlation matrix computation."""
    return log_returns.corr()


def plot_correlation_graph(corr_matrix, threshold=0.9):
    G = nx.Graph()

    for i in range(len(corr_matrix.columns)):
        for j in range(i + 1, len(corr_matrix.columns)):
            corr = corr_matrix.iloc[i, j]
            if abs(corr) >= threshold:
                G.add_edge(corr_matrix.columns[i], corr_matrix.columns[j], weight=corr)

    plt.figure(figsize=(12, 12))
    nx.draw_spring(G, with_labels=False, node_size=30, edge_color='gray')
    plt.title("High-Correlation Network Graph")
    plt.show()


def save_correlation_heatmap(corr_matrix: pd.DataFrame, path: str):
    """Save heatmap of correlation matrix."""
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr_matrix, cmap="coolwarm", center=0, square=True, cbar_kws={"shrink": 0.5})
    plt.title("Correlation Matrix (Log Returns)")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def run_correlation_model(corr_matrix_path: str = CORR_MATRIX_PATH):
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2023, 12, 31)  # for training

    tickers = get_all_tickers_via_api()
    price_df = load_all_close_price_via_api(tickers, start_date, end_date)

    clean_df = price_df.dropna(axis=1, thresh=int(0.9 * len(price_df)))  # keep cols with ≥90% data

    # Drop any rows that still have NaNs
    clean_df = clean_df.dropna()

    corr_matrix = compute_correlation_matrix(clean_df)

    corr_matrix.to_csv(corr_matrix_path)
    print("✅ Correlation matrix saved")

    return corr_matrix

    # na_counts = price_df.isna().sum()
    # print(na_counts.sort_values(ascending=False).head(20))


if __name__ == "__main__":
    run_correlation_model()

