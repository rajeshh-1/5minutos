from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


LIVE_CONFIRMATION_TOKEN = "I_UNDERSTAND_THE_RISK"


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_bool(raw: str) -> bool:
    txt = str(raw).strip().lower()
    if txt in {"1", "true", "yes", "on"}:
        return True
    if txt in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {raw!r}")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _load_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _build_default_session_metrics(gate_payload: dict[str, Any]) -> dict[str, float]:
    selected = gate_payload.get("selected_metrics", {})
    if not isinstance(selected, dict):
        selected = {}
    return {
        "hedge_failed_rate": _safe_float(selected.get("hedge_failed_rate"), 0.0),
        "max_drawdown_pct": _safe_float(selected.get("max_drawdown_pct"), 0.0),
        "p99_loss": _safe_float(selected.get("p99_loss"), 0.0),
        "pnl_per_trade": _safe_float(selected.get("pnl_per_trade"), 0.0),
    }


def run_controlled_rollout(
    *,
    config_file: str | Path,
    promotion_gate_file: str | Path,
    mode: str,
    enable_live: bool,
    confirm_live: str,
    runtime_sec: int,
    out_dir: str | Path,
    session_metrics_file: str | Path | None = None,
) -> dict[str, Any]:
    cfg = _load_json(config_file)
    gate = _load_json(promotion_gate_file)
    mode_norm = str(mode).strip().lower()
    if mode_norm not in {"paper", "shadow", "live"}:
        raise ValueError("mode must be one of: paper, shadow, live")

    thresholds = cfg.get("thresholds", {}) if isinstance(cfg.get("thresholds"), dict) else {}
    hedge_failed_rate_max = _safe_float(thresholds.get("hedge_failed_rate_max"), 0.02)
    max_drawdown_pct_max = _safe_float(thresholds.get("max_drawdown_pct_max"), 12.0)
    p99_loss_max = _safe_float(thresholds.get("p99_loss_max"), 0.40)
    kill_switch_enabled = bool(cfg.get("kill_switch_enabled", True))
    circuit_breaker_enabled = bool(cfg.get("circuit_breaker_enabled", True))

    session_metrics = _build_default_session_metrics(gate)
    if session_metrics_file:
        loaded_metrics = _load_json(session_metrics_file)
        session_metrics.update(
            {
                "hedge_failed_rate": _safe_float(loaded_metrics.get("hedge_failed_rate"), session_metrics["hedge_failed_rate"]),
                "max_drawdown_pct": _safe_float(loaded_metrics.get("max_drawdown_pct"), session_metrics["max_drawdown_pct"]),
                "p99_loss": _safe_float(loaded_metrics.get("p99_loss"), session_metrics["p99_loss"]),
                "pnl_per_trade": _safe_float(loaded_metrics.get("pnl_per_trade"), session_metrics["pnl_per_trade"]),
            }
        )

    phases: list[dict[str, Any]] = []
    rollback_reason_codes: list[str] = []
    kill_switch_status = "inactive"
    rollback_triggered = False
    final_status = "completed"
    live_orders_sent = False
    started = _iso_now()

    phase0 = {"phase": "phase_0_preflight", "status": "ok", "details": []}
    gate_status = str(gate.get("status", "HOLD")).upper()
    if mode_norm == "live":
        if not enable_live:
            phase0["status"] = "aborted"
            phase0["details"].append("live_not_enabled_explicitly")
        if str(confirm_live) != LIVE_CONFIRMATION_TOKEN:
            phase0["status"] = "aborted"
            phase0["details"].append("live_confirmation_missing_or_invalid")
        if gate_status != "PROMOTE":
            phase0["status"] = "aborted"
            phase0["details"].append("promotion_gate_hold_blocks_live")
    phases.append(phase0)

    if phase0["status"] == "aborted":
        final_status = "aborted"
    else:
        phase1 = {
            "phase": "phase_1_shadow_or_paper",
            "status": "ok",
            "runtime_sec": int(runtime_sec),
            "mode": mode_norm,
            "session_metrics": session_metrics,
        }

        if kill_switch_enabled and session_metrics["hedge_failed_rate"] > hedge_failed_rate_max:
            kill_switch_status = "triggered"
            rollback_triggered = True
            rollback_reason_codes.append("rollback_on_hedge_failed_rate_breach")
        if circuit_breaker_enabled and session_metrics["max_drawdown_pct"] > max_drawdown_pct_max:
            kill_switch_status = "triggered"
            rollback_triggered = True
            rollback_reason_codes.append("rollback_on_max_drawdown_breach")
        if circuit_breaker_enabled and session_metrics["p99_loss"] > p99_loss_max:
            kill_switch_status = "triggered"
            rollback_triggered = True
            rollback_reason_codes.append("rollback_on_p99_loss_breach")

        if rollback_triggered:
            phase1["status"] = "rollback_triggered"
        phases.append(phase1)

        phase2 = {
            "phase": "phase_2_micro_live_candidate",
            "status": "skipped",
            "details": [],
        }
        if mode_norm == "live":
            if rollback_triggered:
                phase2["status"] = "skipped"
                phase2["details"].append("rollback_triggered_before_live")
            else:
                phase2["status"] = "simulated_live_candidate"
                phase2["details"].append("live_orders_remain_disabled_in_this_phase")
                live_orders_sent = False
        phases.append(phase2)

    finished = _iso_now()
    payload = {
        "started_at_utc": started,
        "finished_at_utc": finished,
        "mode": mode_norm,
        "runtime_sec": int(runtime_sec),
        "promotion_gate_status": gate_status,
        "enable_live_flag": bool(enable_live),
        "live_confirmed": bool(str(confirm_live) == LIVE_CONFIRMATION_TOKEN),
        "live_orders_sent": bool(live_orders_sent),
        "kill_switch_status": kill_switch_status,
        "rollback_triggered": bool(rollback_triggered),
        "rollback_reason_codes": rollback_reason_codes,
        "final_status": final_status,
        "phases": phases,
        "session_metrics": session_metrics,
    }

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    output_file = out / "rollout_session.json"
    output_file.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    payload["output_file"] = str(output_file)
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run controlled rollout with promotion gate and live safety locks.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--promotion-gate-file", required=True)
    parser.add_argument("--mode", choices=["paper", "shadow", "live"], default="paper")
    parser.add_argument("--enable-live", default="false", help="Explicit live enable flag: true|false")
    parser.add_argument("--confirm-live", default="")
    parser.add_argument("--runtime-sec", type=int, default=300)
    parser.add_argument("--out-dir", default="reports/live")
    parser.add_argument("--session-metrics-file", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = run_controlled_rollout(
        config_file=args.config,
        promotion_gate_file=args.promotion_gate_file,
        mode=args.mode,
        enable_live=_parse_bool(args.enable_live),
        confirm_live=args.confirm_live,
        runtime_sec=int(args.runtime_sec),
        out_dir=args.out_dir,
        session_metrics_file=(args.session_metrics_file or None),
    )

    print(f"rollout_final_status={payload['final_status']}")
    print(f"promotion_gate_status={payload['promotion_gate_status']}")
    print(f"kill_switch_status={payload['kill_switch_status']}")
    print(f"rollback_triggered={payload['rollback_triggered']}")
    for reason in payload.get("rollback_reason_codes", []):
        print(f"  rollback_reason={reason}")
    print(f"output_file={payload['output_file']}")
    return 0 if payload["final_status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
