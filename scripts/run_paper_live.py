from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import load_strategy_config
from src.runtime.checkpoint_store import CheckpointStore
from src.runtime.paper_live_engine import PaperLiveEngine


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run paper live monitored SOL5M engine over local raw data.")
    parser.add_argument("--config", required=True, help="Path to strategy config JSON.")
    parser.add_argument("--raw-dir", required=True, help="Directory with local raw prices/trades/orderbook files.")
    parser.add_argument("--interval-sec", type=float, default=2.0, help="Cycle interval in seconds.")
    parser.add_argument("--runtime-sec", type=int, default=0, help="Runtime duration (0 = infinite).")
    parser.add_argument("--checkpoint-file", default="reports/live/checkpoints.json")
    parser.add_argument("--out-dir", default="reports/live")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-checkpoint", action="store_true", help="Disable checkpoint persistence.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_strategy_config(args.config)
    checkpoint_store = None if args.no_checkpoint else CheckpointStore(args.checkpoint_file)
    engine = PaperLiveEngine(
        config=cfg,
        raw_dir=args.raw_dir,
        out_dir=args.out_dir,
        interval_sec=float(args.interval_sec),
        seed=int(args.seed),
        checkpoint_store=checkpoint_store,
        use_checkpoint=not bool(args.no_checkpoint),
    )
    print(
        f"[paper-live] starting config={args.config} raw_dir={args.raw_dir} "
        f"interval_sec={float(args.interval_sec)} runtime_sec={int(args.runtime_sec)} "
        f"checkpoint={'disabled' if args.no_checkpoint else args.checkpoint_file}"
    )
    snapshot = engine.run(runtime_sec=int(args.runtime_sec))
    print(f"[paper-live] final_status={snapshot.get('engine_status')}")
    print(f"[paper-live] metrics_file={Path(args.out_dir) / 'metrics_live.json'}")
    print(f"[paper-live] events_file={Path(args.out_dir) / 'events.log'}")
    if not args.no_checkpoint:
        print(f"[paper-live] checkpoint_file={args.checkpoint_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

