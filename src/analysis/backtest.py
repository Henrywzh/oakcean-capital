import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from analysis.utils import load_close_price_via_api, TRADE_PATH


class MeanReversionBacktester:
    def __init__(self, trade_path,):
        self.trades = pd.read_csv(trade_path, parse_dates=["entry_date", "exit_date"])


    def get_price_series(self, ticker):
        return load_close_price_via_api(ticker)


    def backtest(self):
        daily_pnl = {}
        ticker_map = {}

        for _, trade in self.trades.iterrows():
            t1 = trade["ticker_a"]
            t2 = trade["ticker_b"]
            entry = trade["entry_date"]
            exit = trade["exit_date"]
            direction = trade["direction"]

            if t1 not in ticker_map:
                s1 = self.get_price_series(t1)
                ticker_map[t1] = s1
            else:
                s1 = ticker_map[t1]

            if t2 not in ticker_map:
                s2 = self.get_price_series(t2)
                ticker_map[t2] = s2
            else:
                s2 = ticker_map[t2]

            try:
                p1_entry = s1.loc[entry]
                p2_entry = s2.loc[entry]
                p1_exit = s1.loc[exit]
                p2_exit = s2.loc[exit]
            except KeyError:
                continue  # missing data

            # Position sizing: $1 long / short
            w = 1.0

            if direction == "long":
                pnl_series = (s1.loc[entry:exit] / p1_entry - 1) - (s2.loc[entry:exit] / p2_entry - 1)
            else:
                pnl_series = (s2.loc[entry:exit] / p2_entry - 1) - (s1.loc[entry:exit] / p1_entry - 1)

            # Convert to dollar PnL
            pnl_series = pnl_series * w

            for date, pnl in pnl_series.items():
                daily_pnl[date] = daily_pnl.get(date, 0) + pnl

        pnl_df = pd.Series(daily_pnl).sort_index()
        pnl_df = pnl_df.fillna(0)
        self.pnl_series = pnl_df.cumsum()

        return self.pnl_series

    def plot_results(self):
        self.pnl_series.plot(title="Cumulative Strategy PnL")
        plt.xlabel("Date")
        plt.ylabel("Cumulative PnL")
        plt.grid()
        plt.show()

    def evaluate(self):
        daily_ret = self.pnl_series.diff().dropna()
        sharpe = daily_ret.mean() / daily_ret.std() * np.sqrt(252)
        drawdown = (self.pnl_series - self.pnl_series.cummax()).min()
        print(f"📈 Sharpe Ratio: {sharpe:.2f}")
        print(f"📉 Max Drawdown: {drawdown:.2f}")

# === RUN ===
if __name__ == "__main__":
    backtester = MeanReversionBacktester(
        trade_path=TRADE_PATH,
    )
    pnl = backtester.backtest()
    backtester.plot_results()
    backtester.evaluate()
