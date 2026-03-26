from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.execution.polymarket_auth import PolymarketAuthConfig
from src.execution.polymarket_live_executor import LiveOrderResult, PolymarketLiveExecutor


EPS = 1e-9
LIVE_CONFIRMATION_TOKEN = "I_UNDERSTAND_THE_RISK"


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_utc(value: Any) -> datetime | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    txt = txt.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(txt)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class LiveMVPConfig:
    market_scope: str = "SOL5M"
    price_min: float = 0.05
    price_max: float = 0.95
    max_sum_ask: float = 0.99
    entry_cutoff_sec: int = 75
    max_spread: float = 0.02
    min_depth_buffer_mult: float = 1.2
    max_unwind_loss_bps: float = 120.0
    stake_usd_per_leg: float = 1.0
    max_trades_per_market: int = 1
    leg2_timeout_ms: int = 2000
    max_daily_loss_usd: float = 3.0
    poll_interval_sec: float = 0.5
    start_from_end: bool = True
    raw_dir: str = "data/raw"
    report_file: str = "reports/live_mvp_trades.csv"

    def __post_init__(self) -> None:
        if str(self.market_scope).strip().upper() != "SOL5M":
            raise ValueError("market_scope must be SOL5M for live MVP")
        if float(self.price_min) < 0 or float(self.price_max) > 1 or float(self.price_min) >= float(self.price_max):
            raise ValueError("price range must satisfy 0 <= min < max <= 1")
        if float(self.max_sum_ask) <= 0 or float(self.max_sum_ask) > 1:
            raise ValueError("max_sum_ask must be in (0, 1]")
        if int(self.entry_cutoff_sec) < 0:
            raise ValueError("entry_cutoff_sec must be >= 0")
        if float(self.max_spread) < 0 or float(self.max_spread) > 1:
            raise ValueError("max_spread must be in [0, 1]")
        if float(self.min_depth_buffer_mult) <= 0:
            raise ValueError("min_depth_buffer_mult must be > 0")
        if float(self.max_unwind_loss_bps) < 0:
            raise ValueError("max_unwind_loss_bps must be >= 0")
        if float(self.stake_usd_per_leg) <= 0:
            raise ValueError("stake_usd_per_leg must be > 0")
        if int(self.max_trades_per_market) < 1:
            raise ValueError("max_trades_per_market must be >= 1")
        if int(self.leg2_timeout_ms) <= 0:
            raise ValueError("leg2_timeout_ms must be > 0")
        if float(self.max_daily_loss_usd) <= 0:
            raise ValueError("max_daily_loss_usd must be > 0")
        if float(self.poll_interval_sec) <= 0:
            raise ValueError("poll_interval_sec must be > 0")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LiveMVPConfig":
        return cls(
            market_scope=str(payload.get("market_scope", "SOL5M")).strip().upper(),
            price_min=float(payload.get("price_min", 0.05)),
            price_max=float(payload.get("price_max", 0.95)),
            max_sum_ask=float(payload.get("max_sum_ask", 0.99)),
            entry_cutoff_sec=int(payload.get("entry_cutoff_sec", 75)),
            max_spread=float(payload.get("max_spread", 0.02)),
            min_depth_buffer_mult=float(payload.get("min_depth_buffer_mult", 1.2)),
            max_unwind_loss_bps=float(payload.get("max_unwind_loss_bps", 120.0)),
            stake_usd_per_leg=float(payload.get("stake_usd_per_leg", 1.0)),
            max_trades_per_market=int(payload.get("max_trades_per_market", 1)),
            leg2_timeout_ms=int(payload.get("leg2_timeout_ms", 2000)),
            max_daily_loss_usd=float(payload.get("max_daily_loss_usd", 3.0)),
            poll_interval_sec=float(payload.get("poll_interval_sec", 0.5)),
            start_from_end=bool(payload.get("start_from_end", True)),
            raw_dir=str(payload.get("raw_dir", "data/raw")),
            report_file=str(payload.get("report_file", "reports/live_mvp_trades.csv")),
        )


@dataclass
class PendingTrade:
    market_key: str
    opened_ts_utc: str
    opened_at_ms: int
    deadline_ms: int
    leg1_side: str  # yes|no
    leg1_ask: float
    leg1_bid: float | None
    sum_ask_entry: float
    leg1_token_id: str = ""
    leg2_token_id: str = ""
    quantity: float = 0.0
    order_id_leg1: str = ""
    order_id_leg2: str = ""
    fill_status_leg1: str = ""
    fill_status_leg2: str = ""


def load_live_mvp_config(path: str | Path) -> LiveMVPConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("live_mvp config must be a JSON object")
    return LiveMVPConfig.from_dict(payload)


class LiveMVPRunner:
    def __init__(
        self,
        *,
        cfg: LiveMVPConfig,
        runtime_sec: int = 0,
        raw_dir_override: str | Path | None = None,
        report_file_override: str | Path | None = None,
        executor: PolymarketLiveExecutor | None = None,
        enable_live: bool = False,
        dry_run: bool = True,
    ) -> None:
        self.cfg = cfg
        self.runtime_sec = max(0, int(runtime_sec))
        self.raw_dir = Path(raw_dir_override) if raw_dir_override else Path(cfg.raw_dir)
        self.report_file = Path(report_file_override) if report_file_override else Path(cfg.report_file)
        self.offsets: dict[str, int] = {}
        self.pending_by_market: dict[str, PendingTrade] = {}
        self.trades_closed_by_market: Counter[str] = Counter()
        self.daily_pnl = 0.0
        self.rows_processed = 0
        self.actions_logged = 0
        self._offsets_bootstrapped = False
        self.enable_live = bool(enable_live)
        self.dry_run = bool(dry_run)
        self.executor = executor
        self.kill_switch_triggered = False
        self.kill_switch_reason = ""
        self._init_report_file()

    def _init_report_file(self) -> None:
        self.report_file.parent.mkdir(parents=True, exist_ok=True)
        if self.report_file.exists() and self.report_file.stat().st_size > 0:
            return
        with self.report_file.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "ts_utc",
                    "market_key",
                    "action",
                    "status",
                    "reason_code",
                    "leg1_side",
                    "yes_ask",
                    "no_ask",
                    "sum_ask",
                    "seconds_to_close",
                    "leg1_price",
                    "leg2_price",
                    "unwind_price",
                    "order_id_leg1",
                    "order_id_leg2",
                    "fill_status_leg1",
                    "fill_status_leg2",
                    "hedge_latency_ms",
                    "unwind_triggered",
                    "pnl_delta",
                    "pnl_total",
                    "note",
                ],
            )
            writer.writeheader()

    def _append_action(
        self,
        *,
        ts_utc: str,
        market_key: str,
        action: str,
        status: str,
        reason_code: str,
        leg1_side: str = "",
        yes_ask: float | None = None,
        no_ask: float | None = None,
        sum_ask: float | None = None,
        seconds_to_close: int | None = None,
        leg1_price: float | None = None,
        leg2_price: float | None = None,
        unwind_price: float | None = None,
        order_id_leg1: str = "",
        order_id_leg2: str = "",
        fill_status_leg1: str = "",
        fill_status_leg2: str = "",
        hedge_latency_ms: int | None = None,
        unwind_triggered: bool | None = None,
        pnl_delta: float = 0.0,
        note: str = "",
    ) -> None:
        self.report_file.parent.mkdir(parents=True, exist_ok=True)
        with self.report_file.open("a", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "ts_utc",
                    "market_key",
                    "action",
                    "status",
                    "reason_code",
                    "leg1_side",
                    "yes_ask",
                    "no_ask",
                    "sum_ask",
                    "seconds_to_close",
                    "leg1_price",
                    "leg2_price",
                    "unwind_price",
                    "order_id_leg1",
                    "order_id_leg2",
                    "fill_status_leg1",
                    "fill_status_leg2",
                    "hedge_latency_ms",
                    "unwind_triggered",
                    "pnl_delta",
                    "pnl_total",
                    "note",
                ],
            )
            writer.writerow(
                {
                    "ts_utc": ts_utc,
                    "market_key": market_key,
                    "action": action,
                    "status": status,
                    "reason_code": reason_code,
                    "leg1_side": leg1_side,
                    "yes_ask": "" if yes_ask is None else round(float(yes_ask), 8),
                    "no_ask": "" if no_ask is None else round(float(no_ask), 8),
                    "sum_ask": "" if sum_ask is None else round(float(sum_ask), 8),
                    "seconds_to_close": "" if seconds_to_close is None else int(seconds_to_close),
                    "leg1_price": "" if leg1_price is None else round(float(leg1_price), 8),
                    "leg2_price": "" if leg2_price is None else round(float(leg2_price), 8),
                    "unwind_price": "" if unwind_price is None else round(float(unwind_price), 8),
                    "order_id_leg1": str(order_id_leg1 or ""),
                    "order_id_leg2": str(order_id_leg2 or ""),
                    "fill_status_leg1": str(fill_status_leg1 or ""),
                    "fill_status_leg2": str(fill_status_leg2 or ""),
                    "hedge_latency_ms": "" if hedge_latency_ms is None else int(hedge_latency_ms),
                    "unwind_triggered": "" if unwind_triggered is None else bool(unwind_triggered),
                    "pnl_delta": round(float(pnl_delta), 10),
                    "pnl_total": round(float(self.daily_pnl), 10),
                    "note": note,
                }
            )
        self.actions_logged += 1

    def _iter_orderbook_files(self) -> list[Path]:
        preferred = self.raw_dir / "sol5m"
        files: list[Path] = []
        if preferred.exists():
            files.extend(sorted(preferred.glob("orderbook_*.jsonl")))
        if self.raw_dir.exists():
            files.extend(sorted(self.raw_dir.glob("orderbook_*.jsonl")))
        seen: set[str] = set()
        unique: list[Path] = []
        for path in files:
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            unique.append(path)
        return unique

    def _bootstrap_offsets_from_end(self) -> None:
        if self._offsets_bootstrapped or not bool(self.cfg.start_from_end):
            return
        for path in self._iter_orderbook_files():
            key = str(path.resolve())
            try:
                self.offsets[key] = int(path.stat().st_size)
            except Exception:
                self.offsets[key] = 0
        self._offsets_bootstrapped = True

    def _read_new_rows(self, path: Path) -> list[dict[str, Any]]:
        key = str(path.resolve())
        old = int(self.offsets.get(key, 0))
        if not path.exists():
            return []
        size = path.stat().st_size
        if size < old:
            old = 0
        with path.open("rb") as fh:
            fh.seek(old, io.SEEK_SET)
            chunk = fh.read()
            new_offset = fh.tell()
        self.offsets[key] = new_offset
        if not chunk:
            return []
        text = chunk.decode("utf-8", errors="ignore")
        rows: list[dict[str, Any]] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
        return rows

    def _first_level(self, levels: Any) -> tuple[float | None, float | None]:
        if not isinstance(levels, list) or not levels:
            return None, None
        item = levels[0]
        if not isinstance(item, dict):
            return None, None
        return _safe_float(item.get("price"), default=None), _safe_float(item.get("size"), default=None)

    def _seconds_to_close(self, *, ts_utc: str, market_key: str) -> int:
        ts = _parse_utc(ts_utc)
        if ts is None or "_" not in market_key:
            return 0
        close_raw = market_key.split("_", 1)[1]
        close_dt = _parse_utc(close_raw)
        if close_dt is None:
            return 0
        return max(0, int((close_dt - ts).total_seconds()))

    def _extract_snapshot(self, row: dict[str, Any]) -> dict[str, Any] | None:
        market_key = str(row.get("market_key", "")).strip()
        ts_utc = str(row.get("timestamp_utc", "")).strip()
        if not market_key or not ts_utc:
            return None

        yes_ask, yes_ask_size = self._first_level(row.get("yes_asks"))
        yes_bid, _ = self._first_level(row.get("yes_bids"))
        no_ask, no_ask_size = self._first_level(row.get("no_asks"))
        no_bid, _ = self._first_level(row.get("no_bids"))
        if yes_ask is None or no_ask is None:
            return None

        sum_ask = float(yes_ask) + float(no_ask)
        yes_spread = None if yes_bid is None else float(yes_ask) - float(yes_bid)
        no_spread = None if no_bid is None else float(no_ask) - float(no_bid)
        return {
            "ts_utc": ts_utc,
            "market_key": market_key,
            "token_up": str(row.get("token_up", "")).strip(),
            "token_down": str(row.get("token_down", "")).strip(),
            "yes_ask": float(yes_ask),
            "yes_bid": None if yes_bid is None else float(yes_bid),
            "yes_ask_size": 0.0 if yes_ask_size is None else float(yes_ask_size),
            "yes_spread": yes_spread,
            "no_ask": float(no_ask),
            "no_bid": None if no_bid is None else float(no_bid),
            "no_ask_size": 0.0 if no_ask_size is None else float(no_ask_size),
            "no_spread": no_spread,
            "sum_ask": float(sum_ask),
            "seconds_to_close": self._seconds_to_close(ts_utc=ts_utc, market_key=market_key),
        }

    def _ts_to_epoch_ms(self, ts_utc: str) -> int:
        ts = _parse_utc(ts_utc)
        if ts is None:
            return int(time.time() * 1000)
        return int(ts.timestamp() * 1000)

    def _within_bounds(self, price: float | None) -> bool:
        if price is None:
            return False
        return float(self.cfg.price_min) - EPS <= float(price) <= float(self.cfg.price_max) + EPS

    def _within_spread(self, spread: float | None) -> bool:
        if spread is None:
            return False
        return float(spread) <= float(self.cfg.max_spread) + EPS

    def _within_depth(self, size: float | None) -> bool:
        if size is None:
            return False
        needed = float(self.cfg.stake_usd_per_leg) * float(self.cfg.min_depth_buffer_mult)
        return float(size) + EPS >= needed

    def _size_for_leg(self, price: float) -> float:
        safe_price = max(EPS, float(price))
        return float(self.cfg.stake_usd_per_leg) / safe_price

    def _live_enabled(self) -> bool:
        return bool(self.enable_live and not self.dry_run and self.executor is not None)

    def _trigger_kill_switch(self, *, ts_utc: str, market_key: str, reason_code: str, note: str) -> None:
        if self.kill_switch_triggered:
            return
        self.kill_switch_triggered = True
        self.kill_switch_reason = reason_code
        cancel_result = {"cancelled": [], "errors": []}
        if self.executor is not None:
            try:
                cancel_result = self.executor.cancel_open_orders()
            except Exception as exc:
                cancel_result = {"cancelled": [], "errors": [str(exc)]}
        self._append_action(
            ts_utc=ts_utc,
            market_key=market_key,
            action="critical_stop",
            status="blocked",
            reason_code=reason_code,
            unwind_triggered=True,
            note=f"{note} | cancel_result={cancel_result}",
        )

    def _calc_unwind_loss_bps(self, *, entry_ask: float, unwind_bid: float | None) -> float:
        if unwind_bid is None or float(entry_ask) <= EPS:
            return float("inf")
        loss = max(0.0, float(entry_ask) - float(unwind_bid))
        return (loss / float(entry_ask)) * 10000.0

    def _pick_leg1(self, *, snapshot: dict[str, Any]) -> tuple[str, float, float | None, str]:
        yes_ask = float(snapshot["yes_ask"])
        no_ask = float(snapshot["no_ask"])
        if yes_ask <= no_ask:
            return "yes", yes_ask, snapshot["yes_bid"], "entered_leg1_yes"
        return "no", no_ask, snapshot["no_bid"], "entered_leg1_no"

    def _attempt_hedge_or_unwind(self, *, snapshot: dict[str, Any], pending: PendingTrade) -> None:
        now_ms = self._ts_to_epoch_ms(snapshot["ts_utc"])
        market_key = pending.market_key
        yes_ask = float(snapshot["yes_ask"])
        no_ask = float(snapshot["no_ask"])
        sum_ask = float(snapshot["sum_ask"])

        if pending.leg1_side == "yes":
            leg2_ask = no_ask
            unwind_bid = snapshot["yes_bid"]
        else:
            leg2_ask = yes_ask
            unwind_bid = snapshot["no_bid"]

        if self._live_enabled():
            try:
                if not pending.leg2_token_id:
                    raise ValueError("missing_leg2_token_id")
                if pending.quantity <= EPS:
                    raise ValueError("invalid_leg_quantity")
                if self._within_bounds(leg2_ask):
                    hedge_result = self.executor.place_leg2_hedge(  # type: ignore[union-attr]
                        token_id=pending.leg2_token_id,
                        price=float(leg2_ask),
                        size=float(pending.quantity),
                        timeout_ms=int(self.cfg.leg2_timeout_ms),
                    )
                else:
                    hedge_result = LiveOrderResult(
                        success=False,
                        order_id="",
                        status="blocked_price_bounds",
                        filled_size=0.0,
                        avg_price=0.0,
                        latency_ms=0,
                        error="leg2_price_out_of_bounds",
                    )
                pending.order_id_leg2 = hedge_result.order_id
                pending.fill_status_leg2 = hedge_result.status
                if hedge_result.success:
                    pnl_delta = (1.0 - float(pending.leg1_ask) - float(hedge_result.avg_price)) * float(
                        self.cfg.stake_usd_per_leg
                    )
                    self.daily_pnl += float(pnl_delta)
                    self.trades_closed_by_market[market_key] += 1
                    self.pending_by_market.pop(market_key, None)
                    self._append_action(
                        ts_utc=snapshot["ts_utc"],
                        market_key=market_key,
                        action="hedge_leg2",
                        status="closed",
                        reason_code="hedge_filled",
                        leg1_side=pending.leg1_side,
                        yes_ask=yes_ask,
                        no_ask=no_ask,
                        sum_ask=sum_ask,
                        seconds_to_close=snapshot["seconds_to_close"],
                        leg1_price=pending.leg1_ask,
                        leg2_price=hedge_result.avg_price if hedge_result.avg_price > 0 else leg2_ask,
                        order_id_leg1=pending.order_id_leg1,
                        order_id_leg2=pending.order_id_leg2,
                        fill_status_leg1=pending.fill_status_leg1 or "submitted",
                        fill_status_leg2=pending.fill_status_leg2,
                        hedge_latency_ms=hedge_result.latency_ms,
                        unwind_triggered=False,
                        pnl_delta=pnl_delta,
                        note="live_hedge_success",
                    )
                    return

                unwind_price_hint = _safe_float(unwind_bid, default=pending.leg1_ask) or float(pending.leg1_ask)
                if not pending.leg1_token_id:
                    raise ValueError("missing_leg1_token_id")
                unwind_result = self.executor.unwind_leg1(  # type: ignore[union-attr]
                    token_id=pending.leg1_token_id,
                    price=float(unwind_price_hint),
                    size=float(pending.quantity),
                )
                pending.fill_status_leg2 = hedge_result.status or "timeout"
                if unwind_result.success:
                    unwind_price = unwind_result.avg_price if unwind_result.avg_price > 0 else float(unwind_price_hint)
                    pnl_delta = (float(unwind_price) - float(pending.leg1_ask)) * float(self.cfg.stake_usd_per_leg)
                    self.daily_pnl += float(pnl_delta)
                    self.trades_closed_by_market[market_key] += 1
                    self.pending_by_market.pop(market_key, None)
                    self._append_action(
                        ts_utc=snapshot["ts_utc"],
                        market_key=market_key,
                        action="unwind_leg1",
                        status="closed",
                        reason_code="leg2_failed_unwind",
                        leg1_side=pending.leg1_side,
                        yes_ask=yes_ask,
                        no_ask=no_ask,
                        sum_ask=sum_ask,
                        seconds_to_close=snapshot["seconds_to_close"],
                        leg1_price=pending.leg1_ask,
                        unwind_price=unwind_price,
                        order_id_leg1=pending.order_id_leg1,
                        order_id_leg2=pending.order_id_leg2,
                        fill_status_leg1=pending.fill_status_leg1 or "submitted",
                        fill_status_leg2=pending.fill_status_leg2,
                        hedge_latency_ms=hedge_result.latency_ms,
                        unwind_triggered=True,
                        pnl_delta=pnl_delta,
                        note=f"live_hedge_failed:{hedge_result.error or hedge_result.status}",
                    )
                    return

                self._trigger_kill_switch(
                    ts_utc=snapshot["ts_utc"],
                    market_key=market_key,
                    reason_code="kill_switch_unwind_failed",
                    note=(
                        f"hedge_error={hedge_result.error or hedge_result.status}; "
                        f"unwind_error={unwind_result.error or unwind_result.status}"
                    ),
                )
                return
            except Exception as exc:
                self._trigger_kill_switch(
                    ts_utc=snapshot["ts_utc"],
                    market_key=market_key,
                    reason_code="kill_switch_execution_error",
                    note=f"exception={exc}",
                )
                return

        if self._within_bounds(leg2_ask):
            pnl_delta = (1.0 - float(pending.leg1_ask) - float(leg2_ask)) * float(self.cfg.stake_usd_per_leg)
            self.daily_pnl += float(pnl_delta)
            self.trades_closed_by_market[market_key] += 1
            self.pending_by_market.pop(market_key, None)
            self._append_action(
                ts_utc=snapshot["ts_utc"],
                market_key=market_key,
                action="hedge_leg2",
                status="closed",
                reason_code="hedge_filled",
                leg1_side=pending.leg1_side,
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=snapshot["seconds_to_close"],
                leg1_price=pending.leg1_ask,
                leg2_price=leg2_ask,
                order_id_leg1=pending.order_id_leg1,
                order_id_leg2=pending.order_id_leg2,
                fill_status_leg1=pending.fill_status_leg1,
                fill_status_leg2="simulated_fill",
                hedge_latency_ms=0,
                unwind_triggered=False,
                pnl_delta=pnl_delta,
                note="mvp_hedge_success",
            )
            return

        if now_ms < int(pending.deadline_ms):
            return

        unwind_price = _safe_float(unwind_bid, default=None)
        if unwind_price is None:
            unwind_price = float(pending.leg1_ask)
        unwind_loss_bps = self._calc_unwind_loss_bps(entry_ask=float(pending.leg1_ask), unwind_bid=unwind_price)
        pnl_delta = (float(unwind_price) - float(pending.leg1_ask)) * float(self.cfg.stake_usd_per_leg)
        self.daily_pnl += float(pnl_delta)
        self.trades_closed_by_market[market_key] += 1
        self.pending_by_market.pop(market_key, None)
        reason_code = "leg2_failed_unwind"
        note = "mvp_timeout_unwind"
        if unwind_loss_bps > float(self.cfg.max_unwind_loss_bps) + EPS:
            reason_code = "leg2_failed_unwind_over_limit"
            note = (
                f"mvp_timeout_unwind loss_bps={unwind_loss_bps:.2f} "
                f"> max_unwind_loss_bps={float(self.cfg.max_unwind_loss_bps):.2f}"
            )
        self._append_action(
            ts_utc=snapshot["ts_utc"],
            market_key=market_key,
            action="unwind_leg1",
            status="closed",
            reason_code=reason_code,
            leg1_side=pending.leg1_side,
            yes_ask=yes_ask,
            no_ask=no_ask,
            sum_ask=sum_ask,
            seconds_to_close=snapshot["seconds_to_close"],
            leg1_price=pending.leg1_ask,
            unwind_price=unwind_price,
            order_id_leg1=pending.order_id_leg1,
            order_id_leg2=pending.order_id_leg2,
            fill_status_leg1=pending.fill_status_leg1,
            fill_status_leg2=pending.fill_status_leg2,
            hedge_latency_ms=0,
            unwind_triggered=True,
            pnl_delta=pnl_delta,
            note=note,
        )

    def _open_leg1(self, *, snapshot: dict[str, Any]) -> None:
        market_key = str(snapshot["market_key"])
        ts_utc = str(snapshot["ts_utc"])
        now_ms = self._ts_to_epoch_ms(ts_utc)
        yes_ask = float(snapshot["yes_ask"])
        no_ask = float(snapshot["no_ask"])
        leg1_side, leg1_ask, leg1_bid, reason_code = self._pick_leg1(snapshot=snapshot)
        token_up = str(snapshot.get("token_up", "")).strip()
        token_down = str(snapshot.get("token_down", "")).strip()
        leg1_token_id = token_up if leg1_side == "yes" else token_down
        leg2_token_id = token_down if leg1_side == "yes" else token_up
        leg_quantity = self._size_for_leg(leg1_ask)
        order_id_leg1 = ""
        fill_status_leg1 = "simulated_open"

        if self._live_enabled():
            if not leg1_token_id or not leg2_token_id:
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_missing_token_ids",
                    leg1_side=leg1_side,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    sum_ask=float(snapshot["sum_ask"]),
                    seconds_to_close=int(snapshot["seconds_to_close"]),
                    note="token_up/token_down absent in snapshot",
                )
                return
            try:
                leg1_result = self.executor.place_leg1_order(  # type: ignore[union-attr]
                    token_id=leg1_token_id,
                    price=float(leg1_ask),
                    size=float(leg_quantity),
                )
            except Exception as exc:
                self._trigger_kill_switch(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    reason_code="kill_switch_leg1_submit_error",
                    note=f"exception={exc}",
                )
                return
            if not leg1_result.success:
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_leg1_rejected",
                    leg1_side=leg1_side,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    sum_ask=float(snapshot["sum_ask"]),
                    seconds_to_close=int(snapshot["seconds_to_close"]),
                    order_id_leg1=leg1_result.order_id,
                    fill_status_leg1=leg1_result.status,
                    note=f"leg1_error={leg1_result.error}",
                )
                return
            order_id_leg1 = leg1_result.order_id
            fill_status_leg1 = leg1_result.status or "submitted"

        pending = PendingTrade(
            market_key=market_key,
            opened_ts_utc=ts_utc,
            opened_at_ms=now_ms,
            deadline_ms=now_ms + int(self.cfg.leg2_timeout_ms),
            leg1_side=leg1_side,
            leg1_ask=float(leg1_ask),
            leg1_bid=None if leg1_bid is None else float(leg1_bid),
            sum_ask_entry=float(snapshot["sum_ask"]),
            leg1_token_id=leg1_token_id,
            leg2_token_id=leg2_token_id,
            quantity=float(leg_quantity),
            order_id_leg1=order_id_leg1,
            fill_status_leg1=fill_status_leg1,
        )
        self.pending_by_market[market_key] = pending
        self._append_action(
            ts_utc=ts_utc,
            market_key=market_key,
            action="enter_leg1",
            status="open",
            reason_code=reason_code,
            leg1_side=leg1_side,
            yes_ask=yes_ask,
            no_ask=no_ask,
            sum_ask=float(snapshot["sum_ask"]),
            seconds_to_close=int(snapshot["seconds_to_close"]),
            leg1_price=leg1_ask,
            order_id_leg1=order_id_leg1,
            fill_status_leg1=fill_status_leg1,
            unwind_triggered=False,
            note="mvp_entry_live" if self._live_enabled() else "mvp_entry",
        )

    def _process_snapshot(self, snapshot: dict[str, Any]) -> None:
        market_key = str(snapshot["market_key"])
        ts_utc = str(snapshot["ts_utc"])
        yes_ask = float(snapshot["yes_ask"])
        no_ask = float(snapshot["no_ask"])
        sum_ask = float(snapshot["sum_ask"])
        seconds_to_close = int(snapshot["seconds_to_close"])

        existing = self.pending_by_market.get(market_key)
        if existing is not None:
            self._attempt_hedge_or_unwind(snapshot=snapshot, pending=existing)
            return

        if self.kill_switch_triggered:
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="kill_switch_active",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                unwind_triggered=True,
                note=self.kill_switch_reason or "kill_switch_triggered",
            )
            return

        if float(self.daily_pnl) <= -float(self.cfg.max_daily_loss_usd):
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_daily_loss",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=f"daily_pnl={self.daily_pnl:.6f} <= -max_daily_loss_usd={self.cfg.max_daily_loss_usd:.6f}",
            )
            return

        if int(self.trades_closed_by_market.get(market_key, 0)) >= int(self.cfg.max_trades_per_market):
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_trade_limit",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=(
                    f"trades_closed={int(self.trades_closed_by_market.get(market_key, 0))} "
                    f">= max_trades_per_market={int(self.cfg.max_trades_per_market)}"
                ),
            )
            return

        if not self._within_bounds(yes_ask) or not self._within_bounds(no_ask):
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_price_bounds",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=f"required_range=[{self.cfg.price_min:.2f},{self.cfg.price_max:.2f}]",
            )
            return

        if int(seconds_to_close) <= int(self.cfg.entry_cutoff_sec):
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_by_cutoff",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=f"seconds_to_close={seconds_to_close} <= entry_cutoff_sec={int(self.cfg.entry_cutoff_sec)}",
            )
            return

        yes_spread = _safe_float(snapshot.get("yes_spread"), default=None)
        no_spread = _safe_float(snapshot.get("no_spread"), default=None)
        if not self._within_spread(yes_spread) or not self._within_spread(no_spread):
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_by_spread",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=(
                    f"yes_spread={yes_spread} no_spread={no_spread} "
                    f"max_spread={float(self.cfg.max_spread):.6f}"
                ),
            )
            return

        yes_size = _safe_float(snapshot.get("yes_ask_size"), default=0.0)
        no_size = _safe_float(snapshot.get("no_ask_size"), default=0.0)
        if not self._within_depth(yes_size) or not self._within_depth(no_size):
            needed = float(self.cfg.stake_usd_per_leg) * float(self.cfg.min_depth_buffer_mult)
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_by_depth",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=f"yes_size={yes_size} no_size={no_size} needed={needed:.6f}",
            )
            return

        if float(sum_ask) > float(self.cfg.max_sum_ask) + EPS:
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_sum_ask",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=f"sum_ask={sum_ask:.6f} > max_sum_ask={self.cfg.max_sum_ask:.6f}",
            )
            return

        leg1_side, leg1_ask, leg1_bid, _ = self._pick_leg1(snapshot=snapshot)
        unwind_loss_bps = self._calc_unwind_loss_bps(entry_ask=leg1_ask, unwind_bid=leg1_bid)
        if unwind_loss_bps > float(self.cfg.max_unwind_loss_bps) + EPS:
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_unwind_risk",
                leg1_side=leg1_side,
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=(
                    f"unwind_loss_bps={unwind_loss_bps:.2f} "
                    f"> max_unwind_loss_bps={float(self.cfg.max_unwind_loss_bps):.2f}"
                ),
            )
            return

        self._open_leg1(snapshot=snapshot)
        pending = self.pending_by_market.get(market_key)
        if pending is not None:
            self._attempt_hedge_or_unwind(snapshot=snapshot, pending=pending)

    def run(self) -> int:
        mode = "live" if self._live_enabled() else ("dry-run" if self.dry_run else "paper")
        print(
            f"[live-mvp] start mode={mode} market={self.cfg.market_scope} stake={self.cfg.stake_usd_per_leg} "
            f"sum_ask<={self.cfg.max_sum_ask} timeout_ms={self.cfg.leg2_timeout_ms} "
            f"max_daily_loss_usd={self.cfg.max_daily_loss_usd} "
            f"cutoff_sec={self.cfg.entry_cutoff_sec} max_spread={self.cfg.max_spread} "
            f"depth_mult={self.cfg.min_depth_buffer_mult} max_unwind_loss_bps={self.cfg.max_unwind_loss_bps}"
        )
        print(f"[live-mvp] raw_dir={self.raw_dir} report_file={self.report_file}")
        if self.cfg.start_from_end:
            print("[live-mvp] start_from_end=true (ignora historico e processa apenas novos ticks)")
        started = time.time()
        try:
            while True:
                self._bootstrap_offsets_from_end()
                files = self._iter_orderbook_files()
                new_rows_total = 0
                for path in files:
                    rows = self._read_new_rows(path)
                    new_rows_total += len(rows)
                    for row in rows:
                        snap = self._extract_snapshot(row)
                        if snap is None:
                            continue
                        self.rows_processed += 1
                        self._process_snapshot(snap)

                print(
                    f"{_iso_utc_now()} | rows={new_rows_total} processed={self.rows_processed} "
                    f"open_pending={len(self.pending_by_market)} closed_trades={sum(self.trades_closed_by_market.values())} "
                    f"daily_pnl={self.daily_pnl:.6f}"
                )

                if self.kill_switch_triggered:
                    print(f"[live-mvp] kill-switch triggered reason={self.kill_switch_reason}")
                    break

                if self.runtime_sec > 0 and (time.time() - started) >= self.runtime_sec:
                    break
                time.sleep(float(self.cfg.poll_interval_sec))
        except KeyboardInterrupt:
            print("[live-mvp] interrupted by Ctrl+C")

        print(
            f"[live-mvp] finished rows_processed={self.rows_processed} "
            f"closed_trades={sum(self.trades_closed_by_market.values())} daily_pnl={self.daily_pnl:.6f} "
            f"actions_logged={self.actions_logged}"
        )
        print(f"[live-mvp] report_file={self.report_file}")
        if self.kill_switch_triggered:
            return 2
        return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run minimal SOL5M live MVP (rule -> entry -> hedge/unwind -> log).")
    parser.add_argument("--config", default="configs/live_mvp_sol5m.json")
    parser.add_argument("--runtime-sec", type=int, default=0, help="0 = run until Ctrl+C")
    parser.add_argument("--raw-dir", default="", help="Optional override for config raw_dir")
    parser.add_argument("--report-file", default="", help="Optional override for config report_file")
    parser.add_argument("--enable-live", default="false", help="true|false (real orders only when true + confirm token)")
    parser.add_argument("--confirm-live", default="", help="Required token for live: I_UNDERSTAND_THE_RISK")
    parser.add_argument("--dry-run", default="true", help="true|false")
    parser.add_argument("--env-file", default=".env", help="Path to env file for live mode")
    return parser.parse_args(argv)


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_live_mvp_config(args.config)
    enable_live = _parse_bool(args.enable_live, default=False)
    dry_run = _parse_bool(args.dry_run, default=True)
    if enable_live and str(args.confirm_live).strip() != LIVE_CONFIRMATION_TOKEN:
        print("[live-mvp] abort: invalid --confirm-live token for live mode")
        return 2

    live_orders_enabled = bool(enable_live and not dry_run)
    executor: PolymarketLiveExecutor | None = None
    if live_orders_enabled:
        try:
            auth = PolymarketAuthConfig.from_env(env_file=str(args.env_file))
        except Exception as exc:
            print(f"[live-mvp] abort: env validation failed: {exc}")
            return 2
        executor = PolymarketLiveExecutor(auth=auth, dry_run=False)
    else:
        executor = PolymarketLiveExecutor(auth=None, dry_run=True)

    runner = LiveMVPRunner(
        cfg=cfg,
        runtime_sec=int(args.runtime_sec),
        raw_dir_override=(args.raw_dir or None),
        report_file_override=(args.report_file or None),
        executor=executor,
        enable_live=enable_live,
        dry_run=dry_run,
    )
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
