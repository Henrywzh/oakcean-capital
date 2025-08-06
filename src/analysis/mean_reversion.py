import pandas as pd
from sklearn.linear_model import LinearRegression
from tqdm import tqdm
import numpy as np
from analysis.utils import CORR_MATRIX_PATH, CLUSTER_LABELS_PATH, TRADE_PATH, load_close_price_via_api


# === Configuration for mean reversion strategy ===
Z_ENTRY = 2
Z_EXIT = 0.5
LOOKBACK = 60  # rolling window size
CORR_LIMIT = 0.99


class MeanReversionStrategy:
    def __init__(
            self, corr_matrix_path, cluster_labels_path, output_path,
                 z_entry=2.0, z_exit=0.5, lookback=60
    ):
        self.corr = pd.read_csv(corr_matrix_path, index_col=0)
        self.cluster_map = self._load_cluster_map(cluster_labels_path)
        self.output_path = output_path
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.lookback = lookback


    def _load_cluster_map(self, path):
        df = pd.read_csv(path)
        cluster_map = {}
        for _, row in df.iterrows():
            cluster_map.setdefault(str(row["cluster"]), []).append(row["ticker"])
        return cluster_map


    def load_price_series(self, ticker):
        return load_close_price_via_api(ticker)


    def calculate_spread_and_zscore(self, x, y):
        x = np.array(x)
        y = np.array(y)
        model = LinearRegression()
        model.fit(x.reshape(-1, 1), y)
        y_pred = model.predict(x.reshape(-1, 1))
        spread = y - y_pred
        if spread.std() == 0:
            return spread, np.full_like(spread, fill_value=np.nan)
        z = (spread - spread.mean()) / spread.std()
        return spread, z


    def run(self):
        trades = []
        for cluster_id, tickers in tqdm(self.cluster_map.items(), desc="Evaluating clusters"):
            for i in range(len(tickers)):
                for j in range(i + 1, len(tickers)):
                    t1, t2 = tickers[i], tickers[j]

                    if t1 not in self.corr.columns or t2 not in self.corr.columns:
                        continue
                    if self.corr.at[t1, t2] < CORR_LIMIT:
                        continue

                    print(f'Trying: {t1}, {t2} with corr: {self.corr.at[t1, t2]}')

                    s1 = self.load_price_series(t1)
                    s2 = self.load_price_series(t2)

                    print(f"Loaded price data: {t1} ({len(s1)} pts), {t2} ({len(s2)} pts)")

                    df = pd.concat([s1, s2], axis=1).dropna()
                    df.columns = ["x", "y"]
                    if len(df) < self.lookback:
                        continue

                    position = None
                    entry_date = None
                    entry_spread = None

                    for end in range(self.lookback, len(df)):
                        window = df.iloc[end - self.lookback:end]
                        x = window["x"].values
                        y = window["y"].values

                        spread, z = self.calculate_spread_and_zscore(x, y)
                        if np.isnan(z[-1]):
                            continue

                        z_score = z[-1]
                        date = df.index[end]

                        if position is None:
                            if z_score > self.z_entry:
                                position = "short"
                                entry_date = date
                                entry_spread = spread[-1]
                            elif z_score < -self.z_entry:
                                position = "long"
                                entry_date = date
                                entry_spread = spread[-1]
                        else:
                            if abs(z_score) < self.z_exit:
                                exit_date = date
                                exit_spread = spread[-1]
                                pnl = (entry_spread - exit_spread) if position == "short" else (exit_spread - entry_spread)
                                trades.append({
                                    "ticker_a": t1,
                                    "ticker_b": t2,
                                    "direction": position,
                                    "entry_date": entry_date,
                                    "exit_date": exit_date,
                                    "entry_spread": entry_spread,
                                    "exit_spread": exit_spread,
                                    "spread_pnl": pnl
                                })
                                position = None

        trades_df = pd.DataFrame(trades)
        trades_df.to_csv(self.output_path, index=False)
        print("✅ Mean-reversion trades saved to:", self.output_path)


if __name__ == "__main__":
    strategy = MeanReversionStrategy(
        corr_matrix_path=CORR_MATRIX_PATH,
        cluster_labels_path=CLUSTER_LABELS_PATH,
        output_path=TRADE_PATH,
        z_entry=Z_ENTRY,
        z_exit=Z_EXIT,
        lookback=LOOKBACK
    )
    strategy.run()
