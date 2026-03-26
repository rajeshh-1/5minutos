from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _pick_profiles(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    if not rows:
        return {"conservative": None, "moderate": None, "aggressive": None}
    go_rows = [row for row in rows if str(row.get("go_no_go", "NO_GO")) == "GO"]
    pool = go_rows if go_rows else rows

    conservative = sorted(
        pool,
        key=lambda row: (
            float(row.get("max_drawdown_pct_mean", 999.0)),
            float(row.get("hedge_failed_rate_mean", 999.0)),
            -float(row.get("robustness_score", -999.0)),
        ),
    )[0]
    moderate = sorted(pool, key=lambda row: float(row.get("robustness_score", -999.0)), reverse=True)[0]
    aggressive = sorted(
        pool,
        key=lambda row: (
            float(row.get("pnl_per_trade_mean", -999.0)),
            float(row.get("robustness_score", -999.0)),
        ),
        reverse=True,
    )[0]
    return {"conservative": conservative, "moderate": moderate, "aggressive": aggressive}


def _short_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "config_id": row.get("config_id"),
        "phase": row.get("phase"),
        "go_no_go": row.get("go_no_go"),
        "robustness_score": row.get("robustness_score"),
        "pnl_per_trade_mean": row.get("pnl_per_trade_mean"),
        "hedge_success_rate_mean": row.get("hedge_success_rate_mean"),
        "hedge_failed_rate_mean": row.get("hedge_failed_rate_mean"),
        "max_drawdown_pct_mean": row.get("max_drawdown_pct_mean"),
        "p99_loss_mean": row.get("p99_loss_mean"),
        "pnl_per_trade_std": row.get("pnl_per_trade_std"),
        "hedge_success_rate_std": row.get("hedge_success_rate_std"),
    }


def build_stability_report(
    *,
    tuning_results_json: str | Path,
    out_dir: str | Path,
    min_runtime_min: int,
) -> dict[str, Any]:
    payload = json.loads(Path(tuning_results_json).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("tuning_results_json must contain a JSON array")
    rows = [row for row in payload if isinstance(row, dict)]
    rows_sorted = sorted(rows, key=lambda row: float(row.get("robustness_score", -999.0)), reverse=True)
    top10 = rows_sorted[:10]
    go_rows = [row for row in rows_sorted if str(row.get("go_no_go", "NO_GO")) == "GO"]
    profiles = _pick_profiles(rows_sorted)

    metrics_payload = {
        "runtime_requirement_minutes": int(min_runtime_min),
        "rows_total": len(rows_sorted),
        "rows_go": len(go_rows),
        "rows_no_go": max(0, len(rows_sorted) - len(go_rows)),
        "top10": [_short_row(row) for row in top10],
        "recommended_profiles": {name: (_short_row(row) if row else None) for name, row in profiles.items()},
    }

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    metrics_path = out / "stability_metrics.json"
    summary_path = out / "stability_summary.md"
    metrics_path.write_text(json.dumps(metrics_payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def _line_for(name: str, row: dict[str, Any] | None) -> str:
        if row is None:
            return f"- {name}: n/a"
        return (
            f"- {name}: `{row.get('config_id')}` "
            f"(score={row.get('robustness_score')}, go={row.get('go_no_go')}, "
            f"pnl/trade={row.get('pnl_per_trade_mean')}, dd={row.get('max_drawdown_pct_mean')}%)"
        )

    summary_lines: list[str] = []
    summary_lines.append("# Stability Summary")
    summary_lines.append("")
    summary_lines.append(f"- runtime_requirement_minutes: {int(min_runtime_min)}")
    summary_lines.append(f"- rows_total: {len(rows_sorted)}")
    summary_lines.append(f"- rows_go: {len(go_rows)}")
    summary_lines.append("")
    summary_lines.append("## Top 10 by Robustness")
    summary_lines.append("")
    for idx, row in enumerate(top10, start=1):
        summary_lines.append(
            f"{idx}. `{row.get('config_id')}` | score={row.get('robustness_score')} "
            f"| go={row.get('go_no_go')} | pnl/trade={row.get('pnl_per_trade_mean')} "
            f"| hedge_failed={row.get('hedge_failed_rate_mean')} | dd={row.get('max_drawdown_pct_mean')}%"
        )
    summary_lines.append("")
    summary_lines.append("## Recommended Profiles")
    summary_lines.append("")
    summary_lines.append(_line_for("conservative", profiles.get("conservative")))
    summary_lines.append(_line_for("moderate", profiles.get("moderate")))
    summary_lines.append(_line_for("aggressive", profiles.get("aggressive")))
    summary_lines.append("")
    summary_lines.append("## Notes")
    summary_lines.append("")
    summary_lines.append(
        "- Stability penalty is already embedded in `robustness_score` via pnl/std and hedge-success/std terms."
    )
    summary_lines.append("- If `rows_go` is 0, recommendations are best-effort over NO_GO candidates.")
    summary_path.write_text("\n".join(summary_lines).strip() + "\n", encoding="utf-8")

    return {
        "stability_metrics_json": str(metrics_path),
        "stability_summary_md": str(summary_path),
        "rows_total": len(rows_sorted),
        "rows_go": len(go_rows),
        "top_3_profiles": {name: (row.get("config_id") if row else "") for name, row in profiles.items()},
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate stability report from tuning artifacts.")
    parser.add_argument("--tuning-results-json", default="reports/tuning/tuning_results.json")
    parser.add_argument("--out-dir", default="reports/stability")
    parser.add_argument("--min-runtime-min", type=int, default=60)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = build_stability_report(
        tuning_results_json=args.tuning_results_json,
        out_dir=args.out_dir,
        min_runtime_min=int(args.min_runtime_min),
    )
    print(f"rows_total={result['rows_total']}")
    print(f"rows_go={result['rows_go']}")
    print(f"stability_metrics_json={result['stability_metrics_json']}")
    print(f"stability_summary_md={result['stability_summary_md']}")
    for name, config_id in result["top_3_profiles"].items():
        print(f"profile_{name}={config_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
