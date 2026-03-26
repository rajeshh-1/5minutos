from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


REGIME_PARAMS: dict[str, dict[str, float]] = {
    "calm": {"vol": 0.0018, "depth": 350.0, "spread": 0.008},
    "normal": {"vol": 0.0045, "depth": 200.0, "spread": 0.012},
    "stress": {"vol": 0.0100, "depth": 90.0, "spread": 0.020},
}


def _clip(value: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(value)))


def _pick_regime(rng: random.Random, regime: str) -> str:
    if regime != "mixed":
        return regime
    draw = rng.random()
    if draw < 0.50:
        return "normal"
    if draw < 0.80:
        return "calm"
    return "stress"


def generate_synthetic_rows(
    *,
    minutes: int,
    seed: int,
    regime: str,
    start_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    if int(minutes) <= 0:
        raise ValueError("minutes must be > 0")
    if regime not in {"calm", "normal", "stress", "mixed"}:
        raise ValueError("regime must be one of: calm, normal, stress, mixed")

    rng = random.Random(int(seed))
    rows: list[dict[str, Any]] = []
    total_rows = int(minutes) * 60
    start = (start_utc or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0, second=0)
    mid_price = 0.50

    for idx in range(total_rows):
        ts = start + timedelta(seconds=idx)
        ts_epoch = int(ts.timestamp())
        close_epoch = ((ts_epoch // 300) + 1) * 300
        seconds_to_close = max(0, close_epoch - ts_epoch)
        close_ts = datetime.fromtimestamp(close_epoch, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        market_key = f"SOL5M_{close_ts}"

        regime_now = _pick_regime(rng, regime)
        params = REGIME_PARAMS[regime_now]
        volatility = float(params["vol"]) * (0.85 + (0.30 * rng.random()))
        depth = float(params["depth"]) * (0.70 + (0.60 * rng.random()))
        spread = float(params["spread"]) * (0.80 + (0.40 * rng.random()))

        shock = rng.gauss(0.0, volatility)
        mid_price = _clip(mid_price + shock, 0.08, 0.92)

        micro_skew = rng.uniform(-spread / 3.0, spread / 3.0)
        up_price = _clip(mid_price + micro_skew, 0.05, 0.95)
        down_price = _clip((1.0 - mid_price) + (spread - micro_skew), 0.05, 0.95)

        rows.append(
            {
                "timestamp_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "market_key": market_key,
                "up_price": round(up_price, 6),
                "down_price": round(down_price, 6),
                "seconds_to_close": int(seconds_to_close),
                "synthetic_depth": round(depth, 6),
                "synthetic_volatility": round(volatility, 8),
            }
        )
    return rows


def write_synthetic_csv(*, rows: list[dict[str, Any]], out_file: str | Path) -> Path:
    path = Path(out_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "timestamp_utc",
                "market_key",
                "up_price",
                "down_price",
                "seconds_to_close",
                "synthetic_depth",
                "synthetic_volatility",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic SOL5M replay dataset for pessimistic dry runs.")
    parser.add_argument("--minutes", type=int, default=120)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-file", default="data/synthetic/sol5m_synth_replay.csv")
    parser.add_argument("--regime", choices=["calm", "normal", "stress", "mixed"], default="mixed")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows = generate_synthetic_rows(minutes=int(args.minutes), seed=int(args.seed), regime=args.regime)
    output = write_synthetic_csv(rows=rows, out_file=args.out_file)
    print(f"rows_generated={len(rows)}")
    print(f"regime={args.regime}")
    print(f"out_file={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
