from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.runtime.metrics import ReadinessThresholds, evaluate_readiness_from_files


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate SOL5M paper replay readiness with GO/NO_GO decision.")
    parser.add_argument("--summary-file", required=True, help="Replay summary JSON path.")
    parser.add_argument("--trade-log-file", required=True, help="Replay trade log CSV path.")
    parser.add_argument("--reason-codes-file", required=True, help="Replay reason codes CSV path.")
    parser.add_argument("--max-drawdown-pct-threshold", type=float, default=12.0)
    parser.add_argument("--p99-loss-threshold", type=float, default=0.40)
    parser.add_argument("--hedge-failed-rate-threshold", type=float, default=0.02)
    parser.add_argument("--out-file", default="reports/paper_readiness.json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    thresholds = ReadinessThresholds(
        hedge_failed_rate_max=float(args.hedge_failed_rate_threshold),
        max_drawdown_pct_max=float(args.max_drawdown_pct_threshold),
        p99_loss_max=float(args.p99_loss_threshold),
    )
    result = evaluate_readiness_from_files(
        summary_file=args.summary_file,
        trade_log_file=args.trade_log_file,
        reason_codes_file=args.reason_codes_file,
        thresholds=thresholds,
    )

    out_path = Path(args.out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"paper_readiness_status={result['status']}")
    print(f"blockers_count={len(result['blockers'])}")
    for blocker in result["blockers"]:
        print(f"  blocker={blocker}")
    print(f"metrics={json.dumps(result['metrics'], ensure_ascii=True)}")
    print(f"output_file={out_path}")
    return 0 if result["status"] == "GO" else 1


if __name__ == "__main__":
    raise SystemExit(main())

