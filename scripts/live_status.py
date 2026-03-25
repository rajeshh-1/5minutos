from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clear() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live status panel for paper monitored engine.")
    parser.add_argument("--metrics-file", default="reports/live/metrics_live.json")
    parser.add_argument("--checkpoint-file", default="reports/live/checkpoints.json")
    parser.add_argument("--refresh-sec", type=float, default=2.0)
    parser.add_argument("--no-clear", action="store_true", help="Do not clear terminal on refresh.")
    return parser.parse_args(argv)


def _render(metrics: dict[str, Any] | None, checkpoints: dict[str, Any] | None) -> str:
    lines: list[str] = []
    lines.append("=" * 90)
    lines.append(f" PAPER LIVE STATUS | now_utc={_iso_utc_now()}")
    lines.append("=" * 90)
    if not metrics:
        lines.append("metrics: MISSING")
    else:
        status = str(metrics.get("engine_status", "unknown")).upper()
        lines.append(f"engine_status={status}")
        lines.append(f"cycle_id={metrics.get('cycle_id', '-')}")
        lines.append(f"last_cycle_utc={metrics.get('last_cycle_utc', '-')}")
        rows = metrics.get("rows_read_cycle", {})
        lines.append(
            "throughput_rows_cycle="
            f"prices:{rows.get('prices', 0)} trades:{rows.get('trades', 0)} "
            f"orderbook:{rows.get('orderbook', 0)} total:{rows.get('total', 0)}"
        )
        m = metrics.get("metrics", {})
        lines.append(
            "metrics="
            f"hedge_success_rate:{m.get('hedge_success_rate', 0)} "
            f"pnl_total:{m.get('pnl_total', 0)} "
            f"max_drawdown_pct:{m.get('max_drawdown_pct', 0)} "
            f"p99_loss:{m.get('p99_loss', 0)}"
        )
        top = m.get("top_reason_codes", [])
        lines.append(f"top_reason_codes={top[:5]}")
    if checkpoints is None:
        lines.append("checkpoints: MISSING")
    else:
        lines.append(f"checkpoint_files_tracked={len(checkpoints)}")
    lines.append("-" * 90)
    lines.append("Ctrl+C para sair")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    refresh = max(0.2, float(args.refresh_sec))
    metrics_file = Path(args.metrics_file)
    checkpoint_file = Path(args.checkpoint_file)

    try:
        while True:
            metrics = _load_json(metrics_file)
            checkpoints = _load_json(checkpoint_file)
            if not args.no_clear:
                _clear()
            print(_render(metrics, checkpoints))
            time.sleep(refresh)
    except KeyboardInterrupt:
        print("\n[live-status] interrupted by Ctrl+C")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

