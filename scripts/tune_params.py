from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, replace
from itertools import product
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import StrategyConfig, load_strategy_config
from src.runtime.metrics import (
    ReadinessThresholds,
    StabilityThresholds,
    compute_metrics,
    compute_robustness_score,
    evaluate_hardened_go_no_go,
    mean,
    stddev,
)
from src.runtime.replay_engine import load_replay_events, run_replay


@dataclass(frozen=True)
class TuningCandidate:
    phase: str
    target_profit_pct: float
    leg2_timeout_ms: int
    max_unwind_loss_bps: float
    entry_cutoff_sec: int

    @property
    def config_id(self) -> str:
        return (
            f"tp{self.target_profit_pct:.2f}_t{self.leg2_timeout_ms}_"
            f"uw{self.max_unwind_loss_bps:.1f}_cut{self.entry_cutoff_sec}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "config_id": self.config_id,
            "target_profit_pct": self.target_profit_pct,
            "leg2_timeout_ms": self.leg2_timeout_ms,
            "max_unwind_loss_bps": self.max_unwind_loss_bps,
            "entry_cutoff_sec": self.entry_cutoff_sec,
        }


def _clamp_float(value: float, minimum: float) -> float:
    return max(float(minimum), float(value))


def _clamp_int(value: int, minimum: int) -> int:
    return max(int(minimum), int(value))


def _dedup_candidates(candidates: list[TuningCandidate]) -> list[TuningCandidate]:
    seen: set[str] = set()
    out: list[TuningCandidate] = []
    for candidate in candidates:
        if candidate.config_id in seen:
            continue
        seen.add(candidate.config_id)
        out.append(candidate)
    return out


def _pick_deterministic(candidates: list[TuningCandidate], limit: int) -> list[TuningCandidate]:
    if limit <= 0 or len(candidates) <= limit:
        return list(candidates)
    ordered = sorted(candidates, key=lambda c: c.config_id)
    idxs = [int(i * len(ordered) / float(limit)) for i in range(limit)]
    picked = [ordered[min(i, len(ordered) - 1)] for i in idxs]
    return _dedup_candidates(picked)


def build_coarse_candidates(base_cfg: StrategyConfig, *, limit: int) -> list[TuningCandidate]:
    target_values = sorted(
        {
            round(_clamp_float(base_cfg.target_profit_pct + delta, 0.25), 2)
            for delta in (-1.0, -0.5, 0.0, 0.5, 1.0)
        }
    )
    timeout_values = sorted(
        {
            _clamp_int(base_cfg.leg2_timeout_ms + delta, 100)
            for delta in (-400, -200, 0, 200, 400)
        }
    )
    unwind_values = sorted(
        {
            round(_clamp_float(base_cfg.max_unwind_loss_bps + delta, 5.0), 1)
            for delta in (-25.0, -10.0, 0.0, 10.0, 25.0)
        }
    )
    cutoff_values = sorted(
        {
            _clamp_int(base_cfg.entry_cutoff_sec + delta, 0)
            for delta in (-30, -15, 0, 15, 30)
        }
    )

    candidates = [
        TuningCandidate(
            phase="coarse",
            target_profit_pct=float(tp),
            leg2_timeout_ms=int(timeout),
            max_unwind_loss_bps=float(unwind),
            entry_cutoff_sec=int(cutoff),
        )
        for tp, timeout, unwind, cutoff in product(target_values, timeout_values, unwind_values, cutoff_values)
    ]
    return _pick_deterministic(_dedup_candidates(candidates), limit)


def build_refine_candidates(
    base: TuningCandidate,
    *,
    limit: int,
) -> list[TuningCandidate]:
    target_values = sorted(
        {
            round(_clamp_float(base.target_profit_pct + delta, 0.25), 2)
            for delta in (-0.25, 0.0, 0.25)
        }
    )
    timeout_values = sorted(
        {_clamp_int(base.leg2_timeout_ms + delta, 100) for delta in (-200, 0, 200)}
    )
    unwind_values = sorted(
        {
            round(_clamp_float(base.max_unwind_loss_bps + delta, 5.0), 1)
            for delta in (-10.0, 0.0, 10.0)
        }
    )
    cutoff_values = sorted({_clamp_int(base.entry_cutoff_sec + delta, 0) for delta in (-15, 0, 15)})

    candidates = [
        TuningCandidate(
            phase="refine",
            target_profit_pct=float(tp),
            leg2_timeout_ms=int(timeout),
            max_unwind_loss_bps=float(unwind),
            entry_cutoff_sec=int(cutoff),
        )
        for tp, timeout, unwind, cutoff in product(target_values, timeout_values, unwind_values, cutoff_values)
    ]
    return _pick_deterministic(_dedup_candidates(candidates), limit)


def _load_trade_log_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _load_reason_counts(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    with path.open("r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            code = str(row.get("reason_code", "")).strip()
            if not code:
                continue
            counts[code] = int(float(row.get("count", 0) or 0))
    return counts


def _candidate_config(base_cfg: StrategyConfig, candidate: TuningCandidate) -> StrategyConfig:
    return replace(
        base_cfg,
        target_profit_pct=float(candidate.target_profit_pct),
        leg2_timeout_ms=int(candidate.leg2_timeout_ms),
        max_unwind_loss_bps=float(candidate.max_unwind_loss_bps),
        entry_cutoff_sec=int(candidate.entry_cutoff_sec),
    )


def _parse_seed_list(raw: str) -> list[int]:
    values: list[int] = []
    for chunk in str(raw).split(","):
        txt = chunk.strip()
        if not txt:
            continue
        values.append(int(txt))
    if not values:
        raise ValueError("seed list must not be empty")
    return values


def _aggregate_candidate(
    *,
    candidate: TuningCandidate,
    per_seed_metrics: list[dict[str, Any]],
    readiness_thresholds: ReadinessThresholds,
    stability_thresholds: StabilityThresholds,
) -> dict[str, Any]:
    pnl_per_trade_values = [float(item["pnl_per_trade"]) for item in per_seed_metrics]
    hedge_success_values = [float(item["hedge_success_rate"]) for item in per_seed_metrics]
    hedge_failed_values = [float(item["hedge_failed_rate"]) for item in per_seed_metrics]
    max_dd_values = [float(item["max_drawdown_pct"]) for item in per_seed_metrics]
    p99_values = [float(item["p99_loss"]) for item in per_seed_metrics]

    pnl_std = stddev(pnl_per_trade_values)
    hedge_success_std = stddev(hedge_success_values)
    agg_metrics = {
        "pnl_per_trade": mean(pnl_per_trade_values),
        "hedge_success_rate": mean(hedge_success_values),
        "hedge_failed_rate": mean(hedge_failed_values),
        "max_drawdown_pct": mean(max_dd_values),
        "p99_loss": mean(p99_values),
    }
    score = compute_robustness_score(
        pnl_per_trade=agg_metrics["pnl_per_trade"],
        hedge_success_rate=agg_metrics["hedge_success_rate"],
        hedge_failed_rate=agg_metrics["hedge_failed_rate"],
        max_drawdown_pct=agg_metrics["max_drawdown_pct"],
        p99_loss=agg_metrics["p99_loss"],
        pnl_per_trade_std=pnl_std,
        hedge_success_rate_std=hedge_success_std,
    )
    decision = evaluate_hardened_go_no_go(
        metrics=agg_metrics,
        readiness_thresholds=readiness_thresholds,
        stability_thresholds=stability_thresholds,
        pnl_per_trade_std=pnl_std,
        hedge_success_rate_std=hedge_success_std,
    )
    row = {
        **candidate.to_dict(),
        "seeds_count": len(per_seed_metrics),
        "pnl_per_trade_mean": round(agg_metrics["pnl_per_trade"], 10),
        "pnl_per_trade_std": round(pnl_std, 10),
        "hedge_success_rate_mean": round(agg_metrics["hedge_success_rate"], 10),
        "hedge_success_rate_std": round(hedge_success_std, 10),
        "hedge_failed_rate_mean": round(agg_metrics["hedge_failed_rate"], 10),
        "max_drawdown_pct_mean": round(agg_metrics["max_drawdown_pct"], 10),
        "p99_loss_mean": round(agg_metrics["p99_loss"], 10),
        "robustness_score": round(score, 10),
        "go_no_go": decision.status,
        "blockers": decision.blockers,
        "seed_metrics": per_seed_metrics,
    }
    return row


def run_tuning(
    *,
    replay_csv: str | Path,
    base_cfg: StrategyConfig,
    out_dir: str | Path,
    seed_list: list[int],
    event_cap: int | None,
    leg1_side: str,
    coarse_limit: int,
    top_k: int,
    refine_per_top: int,
    readiness_thresholds: ReadinessThresholds,
    stability_thresholds: StabilityThresholds,
) -> dict[str, Any]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    runs_dir = out / "_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    events = load_replay_events(replay_csv, event_cap=event_cap)
    coarse_candidates = build_coarse_candidates(base_cfg, limit=coarse_limit)
    all_candidates = list(coarse_candidates)

    def evaluate_batch(candidates: list[TuningCandidate]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for candidate in candidates:
            per_seed_metrics: list[dict[str, Any]] = []
            cfg_variant = _candidate_config(base_cfg, candidate)
            for seed in seed_list:
                run_out = runs_dir / f"{candidate.config_id}_seed{seed}"
                result = run_replay(
                    events=events,
                    config=cfg_variant,
                    out_dir=run_out,
                    seed=int(seed),
                    leg1_side=leg1_side,
                )
                trade_rows = _load_trade_log_rows(result.trade_log_path)
                reason_counts = _load_reason_counts(result.reason_codes_path)
                metrics = compute_metrics(
                    summary=result.summary,
                    trade_log_rows=trade_rows,
                    reason_code_counts=reason_counts,
                )
                per_seed_metrics.append(
                    {
                        "seed": int(seed),
                        "pnl_per_trade": float(metrics["pnl_per_trade"]),
                        "hedge_success_rate": float(metrics["hedge_success_rate"]),
                        "hedge_failed_rate": float(metrics["hedge_failed_rate"]),
                        "max_drawdown_pct": float(metrics["max_drawdown_pct"]),
                        "p99_loss": float(metrics["p99_loss"]),
                    }
                )
            rows.append(
                _aggregate_candidate(
                    candidate=candidate,
                    per_seed_metrics=per_seed_metrics,
                    readiness_thresholds=readiness_thresholds,
                    stability_thresholds=stability_thresholds,
                )
            )
        return rows

    coarse_rows = evaluate_batch(coarse_candidates)
    coarse_sorted = sorted(coarse_rows, key=lambda row: float(row["robustness_score"]), reverse=True)
    top_coarse_ids = {str(row["config_id"]) for row in coarse_sorted[: max(1, int(top_k))]}
    top_coarse = [c for c in coarse_candidates if c.config_id in top_coarse_ids]

    refine_candidates: list[TuningCandidate] = []
    for candidate in top_coarse:
        refine_candidates.extend(build_refine_candidates(candidate, limit=refine_per_top))
    refine_candidates = _dedup_candidates(refine_candidates)
    refine_rows = evaluate_batch(refine_candidates)

    all_rows = coarse_rows + refine_rows
    all_rows_sorted = sorted(all_rows, key=lambda row: float(row["robustness_score"]), reverse=True)
    top_rows = all_rows_sorted[:10]

    csv_path = out / "tuning_results.csv"
    json_path = out / "tuning_results.json"
    top_path = out / "top_configs.json"

    csv_fields = [
        "phase",
        "config_id",
        "target_profit_pct",
        "leg2_timeout_ms",
        "max_unwind_loss_bps",
        "entry_cutoff_sec",
        "seeds_count",
        "pnl_per_trade_mean",
        "pnl_per_trade_std",
        "hedge_success_rate_mean",
        "hedge_success_rate_std",
        "hedge_failed_rate_mean",
        "max_drawdown_pct_mean",
        "p99_loss_mean",
        "robustness_score",
        "go_no_go",
        "blockers",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=csv_fields)
        writer.writeheader()
        for row in all_rows_sorted:
            writer.writerow(
                {
                    **{k: row[k] for k in csv_fields if k != "blockers"},
                    "blockers": "|".join(str(x) for x in row.get("blockers", [])),
                }
            )

    json_path.write_text(json.dumps(all_rows_sorted, indent=2, ensure_ascii=True), encoding="utf-8")
    top_path.write_text(json.dumps(top_rows, indent=2, ensure_ascii=True), encoding="utf-8")

    return {
        "candidates_coarse": len(coarse_candidates),
        "candidates_refine": len(refine_candidates),
        "results_total": len(all_rows_sorted),
        "tuning_results_csv": str(csv_path),
        "tuning_results_json": str(json_path),
        "top_configs_json": str(top_path),
        "top_5": top_rows[:5],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-tune SOL5M paper parameters over local replay data.")
    parser.add_argument("--input", required=True, help="Replay CSV input file.")
    parser.add_argument("--config", default="configs/paper_sol5m_usd1.json", help="Base strategy config JSON.")
    parser.add_argument("--out-dir", default="reports/tuning")
    parser.add_argument("--seed-list", default="42,99,123", help="Comma separated list of seeds.")
    parser.add_argument("--event-cap", type=int, default=0, help="Optional replay event cap (0 = all).")
    parser.add_argument("--leg1-side", choices=["up", "down", "auto"], default="auto")
    parser.add_argument("--coarse-limit", type=int, default=20)
    parser.add_argument("--top-k", type=int, default=4, help="Top coarse candidates promoted to refine phase.")
    parser.add_argument("--refine-per-top", type=int, default=10, help="Refine candidates per selected coarse config.")
    parser.add_argument("--max-drawdown-pct", type=float, default=12.0)
    parser.add_argument("--max-p99-loss", type=float, default=0.40)
    parser.add_argument("--max-pnl-std", type=float, default=0.02)
    parser.add_argument("--max-hedge-success-std", type=float, default=0.10)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    base_cfg = load_strategy_config(args.config)
    seeds = _parse_seed_list(args.seed_list)
    readiness = ReadinessThresholds(
        hedge_failed_rate_max=0.02,
        max_drawdown_pct_max=float(args.max_drawdown_pct),
        p99_loss_max=float(args.max_p99_loss),
    )
    stability = StabilityThresholds(
        pnl_per_trade_std_max=float(args.max_pnl_std),
        hedge_success_rate_std_max=float(args.max_hedge_success_std),
    )

    result = run_tuning(
        replay_csv=args.input,
        base_cfg=base_cfg,
        out_dir=args.out_dir,
        seed_list=seeds,
        event_cap=int(args.event_cap) if int(args.event_cap) > 0 else None,
        leg1_side=args.leg1_side,
        coarse_limit=int(args.coarse_limit),
        top_k=int(args.top_k),
        refine_per_top=int(args.refine_per_top),
        readiness_thresholds=readiness,
        stability_thresholds=stability,
    )

    print(f"candidates_coarse={result['candidates_coarse']}")
    print(f"candidates_refine={result['candidates_refine']}")
    print(f"results_total={result['results_total']}")
    print(f"tuning_results_csv={result['tuning_results_csv']}")
    print(f"tuning_results_json={result['tuning_results_json']}")
    print(f"top_configs_json={result['top_configs_json']}")
    for idx, row in enumerate(result["top_5"], start=1):
        print(
            f"top{idx}={row['config_id']} score={row['robustness_score']} "
            f"go_no_go={row['go_no_go']} pnl_per_trade_mean={row['pnl_per_trade_mean']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
