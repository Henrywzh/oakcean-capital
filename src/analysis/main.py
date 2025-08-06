from analysis.backtest import MeanReversionBacktester
from analysis.clustering_model import run_clustering_model
from analysis.correlation_model import run_correlation_model
from analysis.mean_reversion import MeanReversionStrategy, Z_ENTRY, Z_EXIT, LOOKBACK
from analysis.utils import CORR_MATRIX_PATH, CLUSTER_LABELS_PATH, TRADE_PATH


def main():
    run_correlation_model()
    run_clustering_model()

    strategy = MeanReversionStrategy(
        corr_matrix_path=CORR_MATRIX_PATH,
        cluster_labels_path=CLUSTER_LABELS_PATH,
        output_path=TRADE_PATH,
        z_entry=Z_ENTRY,
        z_exit=Z_EXIT,
        lookback=LOOKBACK
    )
    strategy.run()

    backtester = MeanReversionBacktester(
        trade_path=TRADE_PATH,
    )
    backtester.backtest()
    backtester.plot_results()
    backtester.evaluate()


if __name__ == '__main__':
    main()
