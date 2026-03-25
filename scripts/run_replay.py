from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import load_strategy_config
from src.runtime.replay_engine import load_replay_events, run_replay


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SOL5M legging replay engine over historical CSV data.")
    parser.add_argument("--input", required=True, help="Input replay CSV file.")
    parser.add_argument("--config", required=True, help="Strategy config JSON (paper mode expected).")
    parser.add_argument("--out-dir", default="reports", help="Output directory for replay artifacts.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for deterministic replay behavior.")
    parser.add_argument("--event-cap", type=int, default=0, help="Optional cap on number of events (0 = all).")
    parser.add_argument("--leg1-side", choices=["up", "down", "auto"], default="auto")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_strategy_config(args.config)
    events = load_replay_events(args.input, event_cap=args.event_cap if args.event_cap > 0 else None)
    result = run_replay(
        events=events,
        config=cfg,
        out_dir=args.out_dir,
        seed=int(args.seed),
        leg1_side=args.leg1_side,
    )

    print(f"events_processed={result.summary['events_processed']}")
    print(f"trades_attempted={result.summary['trades_attempted']}")
    print(f"leg1_opened={result.summary['leg1_opened']}")
    print(f"leg2_hedged={result.summary['leg2_hedged']}")
    print(f"unwind_count={result.summary['unwind_count']}")
    print(f"pnl_total={result.summary['pnl_total']}")
    print(f"summary_file={result.summary_path}")
    print(f"trade_log_file={result.trade_log_path}")
    print(f"reason_codes_file={result.reason_codes_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

