from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.runtime.promotion_gate import HOLD, PromotionThresholds, evaluate_promotion_gate


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate paper-to-live promotion gate.")
    parser.add_argument("--stability-metrics", default="reports/stability/stability_metrics.json")
    parser.add_argument("--top-configs", default="reports/tuning/top_configs.json")
    parser.add_argument("--paper-readiness", default="reports/paper_readiness.json")
    parser.add_argument("--pnl-per-trade-min", type=float, default=0.0)
    parser.add_argument("--hedge-failed-rate-max", type=float, default=0.02)
    parser.add_argument("--max-drawdown-pct-max", type=float, default=12.0)
    parser.add_argument("--p99-loss-max", type=float, default=0.40)
    parser.add_argument("--pnl-per-trade-std-max", type=float, default=0.02)
    parser.add_argument("--hedge-success-rate-std-max", type=float, default=0.10)
    parser.add_argument("--out-file", default="reports/live/promotion_gate.json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    thresholds = PromotionThresholds(
        pnl_per_trade_min=float(args.pnl_per_trade_min),
        hedge_failed_rate_max=float(args.hedge_failed_rate_max),
        max_drawdown_pct_max=float(args.max_drawdown_pct_max),
        p99_loss_max=float(args.p99_loss_max),
        pnl_per_trade_std_max=float(args.pnl_per_trade_std_max),
        hedge_success_rate_std_max=float(args.hedge_success_rate_std_max),
    )
    paper_readiness = str(args.paper_readiness).strip()
    if not paper_readiness:
        paper_readiness = None
    decision = evaluate_promotion_gate(
        stability_metrics_file=args.stability_metrics,
        top_configs_file=args.top_configs,
        paper_readiness_file=paper_readiness,
        thresholds=thresholds,
    )

    out_path = Path(args.out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(decision, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"promotion_status={decision['status']}")
    print(f"blockers_count={len(decision['blockers'])}")
    for blocker in decision["blockers"]:
        print(f"  blocker={blocker}")
    print(f"output_file={out_path}")
    return 0 if decision["status"] != HOLD else 1


if __name__ == "__main__":
    raise SystemExit(main())
