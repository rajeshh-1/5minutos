from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.io.market_data_collector import MarketDataCollector, load_collector_config


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect real SOL5M market data (prices + orderbook + trades).")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--runtime-sec", type=int, default=0, help="0 = run until Ctrl+C")
    parser.add_argument("--book-depth", type=int, default=15)
    parser.add_argument("--trade-limit", type=int, default=150)
    parser.add_argument("--checkpoint-file", default="")
    parser.add_argument(
        "--last-closed-file",
        default="reports/live/last_closed_market.json",
        help="Path for Terminal A to publish the latest closed market payload.",
    )
    parser.add_argument("--no-orderbook", action="store_true")
    parser.add_argument("--no-trades", action="store_true")
    parser.add_argument("--config", default="configs/data_collection_sol5m.json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_collector_config(args.config)
    collector = MarketDataCollector(
        config=cfg,
        raw_dir=args.raw_dir,
        interval_sec=float(args.interval_sec),
        runtime_sec=int(args.runtime_sec),
        book_depth=int(args.book_depth),
        trade_limit=int(args.trade_limit),
        collect_orderbook=not bool(args.no_orderbook),
        collect_trades=not bool(args.no_trades),
        checkpoint_file=(args.checkpoint_file or None),
        last_closed_file=(args.last_closed_file or None),
    )
    return collector.run()


if __name__ == "__main__":
    raise SystemExit(main())
