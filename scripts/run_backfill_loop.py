from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backfill_market_trades import backfill_market_trades
from src.io.backfill_scheduler import BackfillScheduler, parse_utc
from src.io.trade_reconciler import reconcile_trade_files
from src.io.storage_writer import iso_utc


def _date_from_iso(ts_utc: str) -> str:
    dt = parse_utc(ts_utc)
    if dt is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")


def _resolve_realtime_trades_file(*, raw_dir: Path, close_time_utc: str) -> Path:
    day = _date_from_iso(close_time_utc)
    candidate = raw_dir / "sol5m" / f"trades_{day}.jsonl"
    if candidate.exists():
        return candidate
    fallback = sorted((raw_dir / "sol5m").glob("trades_*.jsonl"))
    if fallback:
        return fallback[-1]
    return candidate


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Terminal B loop: backfill closed SOL5M markets and reconcile trades.")
    parser.add_argument("--poll-sec", type=float, default=20.0)
    parser.add_argument("--backfill-interval-sec", type=int, default=300)
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--state-file", default="reports/live/backfill_state.json")
    parser.add_argument("--last-closed-file", default="reports/live/last_closed_market.json")
    parser.add_argument("--config", default="configs/data_collection_sol5m.json")
    parser.add_argument("--reports-dir", default="reports/live")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit.")
    return parser.parse_args(argv)


def run_one_cycle(*, args: argparse.Namespace, scheduler: BackfillScheduler) -> bool:
    market = scheduler.get_pending_market()
    if market is None:
        print(f"{iso_utc()} | [backfill-loop] no_pending_closed_market")
        return False

    print(
        f"{iso_utc()} | [backfill-loop] processing market_key={market.market_key} "
        f"condition_id={market.condition_id}"
    )
    result = backfill_market_trades(
        condition_id=market.condition_id,
        market_key=market.market_key,
        close_time_utc=market.close_time_utc,
        config_path=str(args.config),
        raw_dir=str(args.raw_dir),
        page_size=max(1, int(args.page_size)),
        max_pages=max(0, int(args.max_pages)),
    )

    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    realtime_file = _resolve_realtime_trades_file(raw_dir=Path(args.raw_dir), close_time_utc=market.close_time_utc)
    reconciliation = reconcile_trade_files(
        realtime_file=realtime_file,
        backfill_file=result["trades_file"],
        market_key=market.market_key,
    )
    reconciliation.update(
        {
            "condition_id": market.condition_id,
            "market_key": market.market_key,
            "close_time_utc": market.close_time_utc,
            "realtime_file": str(realtime_file),
            "backfill_file": str(result["trades_file"]),
            "generated_at_utc": iso_utc(),
        }
    )
    recon_file = reports_dir / f"reconciliation_{market.condition_id}.json"
    recon_file.write_text(json.dumps(reconciliation, ensure_ascii=True, indent=2), encoding="utf-8")

    scheduler.mark_processed(
        market,
        backfill_meta_file=str(result["meta_file"]),
        reconciliation_file=str(recon_file),
        rows_backfill=int(reconciliation.get("backfill_count", 0)),
        rows_realtime=int(reconciliation.get("realtime_count", 0)),
    )

    print(
        f"{iso_utc()} | [backfill-loop] done condition_id={market.condition_id} "
        f"backfill={reconciliation['backfill_count']} realtime={reconciliation['realtime_count']} "
        f"coverage_pct={reconciliation['coverage_pct']}"
    )
    return True


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    scheduler = BackfillScheduler(
        last_closed_file=str(args.last_closed_file),
        state_file=str(args.state_file),
        backfill_interval_sec=max(0, int(args.backfill_interval_sec)),
    )
    print(
        f"[backfill-loop] poll_sec={float(args.poll_sec):.1f} "
        f"backfill_interval_sec={int(args.backfill_interval_sec)} "
        f"state_file={args.state_file}"
    )
    try:
        while True:
            try:
                run_one_cycle(args=args, scheduler=scheduler)
            except Exception as exc:  # noqa: BLE001
                print(f"{iso_utc()} | [backfill-loop] warning cycle_error={exc}")
            if args.once:
                break
            time.sleep(max(0.1, float(args.poll_sec)))
    except KeyboardInterrupt:
        print("[backfill-loop] interrupted by Ctrl+C")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

