from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from random import Random


@dataclass
class Bar:
    time: datetime
    close: float
    signal: str


def generate_bars(seed: int, start: datetime, end: datetime) -> list[Bar]:
    rng = Random(seed)
    bars: list[Bar] = []
    price = 100.0
    current = start
    while current <= end:
        drift = rng.uniform(-1.5, 1.8)
        price = max(1.0, price + drift)
        if drift > 0.6:
            signal = "BUY"
        elif drift < -0.6:
            signal = "SELL"
        else:
            signal = "HOLD"
        bars.append(Bar(time=current, close=round(price, 2), signal=signal))
        current += timedelta(days=1)
    return bars


def evaluate(bars: list[Bar]) -> dict[str, float]:
    pnl = 0.0
    wins = 0
    trades = 0
    for prev, cur in zip(bars, bars[1:]):
        ret = cur.close - prev.close
        if prev.signal == "BUY":
            trades += 1
            pnl += ret
            wins += 1 if ret > 0 else 0
        elif prev.signal == "SELL":
            trades += 1
            pnl -= ret
            wins += 1 if ret < 0 else 0

    win_rate = wins / trades if trades else 0.0
    return {
        "trades": float(trades),
        "pnl": round(pnl, 4),
        "win_rate": round(win_rate, 4),
    }


def write_csv(path: Path, bars: list[Bar]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "close", "signal"])
        for bar in bars:
            writer.writerow([bar.time.date().isoformat(), bar.close, bar.signal])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a minimal sentiment-driven backtest.")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--strategy", default="baseline_sentiment")
    parser.add_argument("--out", default="artifacts/backtest_report.csv")
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start)
    end = datetime.fromisoformat(args.end)
    bars = generate_bars(seed=7, start=start, end=end)
    metrics = evaluate(bars)
    write_csv(Path(args.out), bars)

    print(
        f"symbol={args.symbol} strategy={args.strategy} trades={int(metrics['trades'])} "
        f"pnl={metrics['pnl']} win_rate={metrics['win_rate']}"
    )


if __name__ == "__main__":
    main()
