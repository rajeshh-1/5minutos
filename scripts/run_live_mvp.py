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


EPS = 1e-9


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

        yes_ask, _yes_size = self._first_level(row.get("yes_asks"))
        yes_bid, _ = self._first_level(row.get("yes_bids"))
        no_ask, _no_size = self._first_level(row.get("no_asks"))
        no_bid, _ = self._first_level(row.get("no_bids"))
        if yes_ask is None or no_ask is None:
            return None

        sum_ask = float(yes_ask) + float(no_ask)
        return {
            "ts_utc": ts_utc,
            "market_key": market_key,
            "yes_ask": float(yes_ask),
            "yes_bid": None if yes_bid is None else float(yes_bid),
            "no_ask": float(no_ask),
            "no_bid": None if no_bid is None else float(no_bid),
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
                pnl_delta=pnl_delta,
                note="mvp_hedge_success",
            )
            return

        if now_ms < int(pending.deadline_ms):
            return

        unwind_price = _safe_float(unwind_bid, default=None)
        if unwind_price is None:
            unwind_price = float(pending.leg1_ask)
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
            pnl_delta=pnl_delta,
            note="mvp_timeout_unwind",
        )

    def _open_leg1(self, *, snapshot: dict[str, Any]) -> None:
        market_key = str(snapshot["market_key"])
        ts_utc = str(snapshot["ts_utc"])
        now_ms = self._ts_to_epoch_ms(ts_utc)
        yes_ask = float(snapshot["yes_ask"])
        no_ask = float(snapshot["no_ask"])

        if yes_ask <= no_ask:
            leg1_side = "yes"
            leg1_ask = yes_ask
            leg1_bid = snapshot["yes_bid"]
            reason_code = "entered_leg1_yes"
        else:
            leg1_side = "no"
            leg1_ask = no_ask
            leg1_bid = snapshot["no_bid"]
            reason_code = "entered_leg1_no"

        pending = PendingTrade(
            market_key=market_key,
            opened_ts_utc=ts_utc,
            opened_at_ms=now_ms,
            deadline_ms=now_ms + int(self.cfg.leg2_timeout_ms),
            leg1_side=leg1_side,
            leg1_ask=float(leg1_ask),
            leg1_bid=None if leg1_bid is None else float(leg1_bid),
            sum_ask_entry=float(snapshot["sum_ask"]),
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
            note="mvp_entry",
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

        self._open_leg1(snapshot=snapshot)
        pending = self.pending_by_market.get(market_key)
        if pending is not None:
            self._attempt_hedge_or_unwind(snapshot=snapshot, pending=pending)

    def run(self) -> int:
        print(
            f"[live-mvp] start market={self.cfg.market_scope} stake={self.cfg.stake_usd_per_leg} "
            f"sum_ask<={self.cfg.max_sum_ask} timeout_ms={self.cfg.leg2_timeout_ms} "
            f"max_daily_loss_usd={self.cfg.max_daily_loss_usd}"
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
        return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run minimal SOL5M live MVP (rule -> entry -> hedge/unwind -> log).")
    parser.add_argument("--config", default="configs/live_mvp_sol5m.json")
    parser.add_argument("--runtime-sec", type=int, default=0, help="0 = run until Ctrl+C")
    parser.add_argument("--raw-dir", default="", help="Optional override for config raw_dir")
    parser.add_argument("--report-file", default="", help="Optional override for config report_file")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_live_mvp_config(args.config)
    runner = LiveMVPRunner(
        cfg=cfg,
        runtime_sec=int(args.runtime_sec),
        raw_dir_override=(args.raw_dir or None),
        report_file_override=(args.report_file or None),
    )
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
