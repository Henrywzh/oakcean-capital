import pandas as pd
from scipy.spatial.distance import squareform
from sklearn.cluster import AgglomerativeClustering
from scipy.cluster.hierarchy import linkage, dendrogram
import matplotlib.pyplot as plt
from analysis.utils import CORR_MATRIX_PATH, CLUSTER_LABELS_PATH


def run_clustering_model(input_path: str = CORR_MATRIX_PATH, output_path: str = CLUSTER_LABELS_PATH):
    # Load correlation matrix
    correlation = pd.read_csv(input_path, index_col=0)

    # Drop rows and columns with any NaNs (after enforcing symmetry)
    correlation = correlation.dropna(axis=0, how="any")
    correlation = correlation.dropna(axis=1, how="any")

    # Ensure the matrix is symmetric and square by taking common tickers
    common_tickers = correlation.index.intersection(correlation.columns)
    correlation = correlation.loc[common_tickers, common_tickers]

    # Check
    assert correlation.shape[0] == correlation.shape[1], f"{correlation.shape[0]} != {correlation.shape[1]}"

    # Convert to distance matrix
    distance = 1 - correlation

    # Clustering
    n_clusters = 10
    clustering = AgglomerativeClustering(
        metric="precomputed",
        linkage="complete",
        n_clusters=n_clusters
    )

    labels = clustering.fit_predict(distance)

    # Save results
    cluster_df = pd.DataFrame({
        "ticker": correlation.index,
        "cluster": labels
    })
    cluster_df.to_csv(output_path, index=False)

    print("✅ Clustering done. Sample result:")
    print(cluster_df["cluster"].value_counts())

    # Dendrogram plot
    Z = linkage(squareform(distance), method='complete')
    dendrogram(Z, no_labels=True)
    plt.show()


if __name__ == "__main__":
    run_clustering_model()