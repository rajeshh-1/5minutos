from __future__ import annotations

import csv
import io
import json
import random
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.config import TARGET_MODE_NET, StrategyConfig
from src.core.reason_codes import LEG1_OPENED, LEG2_HEDGE_FILLED
from src.runtime.checkpoint_store import CheckpointStore
from src.runtime.metrics import compute_metrics
from src.strategy.state_machine import LeggingState, LeggingStateMachine


@dataclass
class MarketSession:
    machine: LeggingStateMachine
    leg1_side: str
    leg1_open_price: float


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _parse_utc(value: str) -> datetime | None:
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


def _derive_seconds_to_close(row: dict[str, Any]) -> int:
    direct = _safe_int(row.get("seconds_to_close"), default=-1)
    if direct >= 0:
        return direct
    ts = _parse_utc(str(row.get("timestamp_utc", "")))
    market_key = str(row.get("market_key", ""))
    if ts is None or "_" not in market_key:
        return 120
    close_dt = _parse_utc(market_key.split("_", 1)[1])
    if close_dt is None:
        return 120
    return max(0, int((close_dt - ts).total_seconds()))


def _derive_logical_ts(row: dict[str, Any], fallback: int) -> int:
    ts = _parse_utc(str(row.get("timestamp_utc", "")))
    if ts is None:
        return int(fallback)
    return int(ts.timestamp())


def _calc_loss_bps(open_price: float, current_price: float) -> float:
    base = max(1e-12, float(open_price))
    return max(0.0, ((float(open_price) - float(current_price)) / base) * 10000.0)


def _calc_hedged_pnl(cfg: StrategyConfig, p1_price: float, p2_price: float) -> float:
    costs = float(cfg.estimated_costs) if cfg.target_mode == TARGET_MODE_NET else 0.0
    edge = 1.0 - float(p1_price) - float(p2_price) - costs
    return edge * float(cfg.stake_usd_per_leg)


def _calc_unwind_pnl(cfg: StrategyConfig, loss_bps: float) -> float:
    return -(float(loss_bps) / 10000.0) * float(cfg.stake_usd_per_leg)


class PaperLiveEngine:
    def __init__(
        self,
        *,
        config: StrategyConfig,
        raw_dir: str | Path,
        out_dir: str | Path,
        interval_sec: float = 2.0,
        seed: int = 42,
        checkpoint_store: CheckpointStore | None = None,
        use_checkpoint: bool = True,
    ) -> None:
        self.config = config
        self.raw_dir = Path(raw_dir)
        self.out_dir = Path(out_dir)
        self.interval_sec = max(0.1, float(interval_sec))
        self.rng = random.Random(int(seed))
        self.checkpoint_store = checkpoint_store
        self.use_checkpoint = bool(use_checkpoint)

        self.offsets: dict[str, int] = {}
        if self.checkpoint_store is not None and self.use_checkpoint:
            self.offsets = self.checkpoint_store.load()

        self._csv_headers: dict[str, list[str]] = {}
        self.market_sessions: dict[str, MarketSession] = {}
        self.reason_counts: Counter[str] = Counter()
        self.final_state_counts: Counter[str] = Counter()
        self.trade_log_rows: list[dict[str, Any]] = []

        self.trades_attempted = 0
        self.leg1_opened = 0
        self.leg2_hedged = 0
        self.unwind_count = 0
        self.pnl_total = 0.0
        self.closed_trades = 0
        self.cycle_id = 0
        self.last_snapshot: dict[str, Any] = {}

        self.metrics_file = self.out_dir / "metrics_live.json"
        self.events_log_file = self.out_dir / "events.log"
        self.top_file = self.out_dir / "top_policies_live.csv"

    def _resolve_source_files(self) -> dict[str, list[Path]]:
        base_a = self.raw_dir / "sol5m"
        base_b = self.raw_dir
        base = base_a if base_a.exists() else base_b
        out: dict[str, list[Path]] = {"trades": [], "prices": [], "orderbook": []}
        for kind in ("trades", "prices", "orderbook"):
            out[kind].extend(sorted(base.glob(f"{kind}_*.jsonl")))
            out[kind].extend(sorted(base.glob(f"{kind}_*.csv")))
        return out

    def _load_csv_header(self, path: Path) -> list[str]:
        key = str(path.resolve())
        if key in self._csv_headers:
            return self._csv_headers[key]
        with path.open("r", encoding="utf-8", newline="") as fh:
            line = fh.readline().strip()
        header = next(csv.reader([line])) if line else []
        self._csv_headers[key] = header
        return header

    def _read_new_rows(self, path: Path) -> tuple[list[dict[str, Any]], int]:
        key = str(path.resolve())
        old = int(self.offsets.get(key, 0))
        if not path.exists():
            return [], old
        size = path.stat().st_size
        if size < old:
            old = 0
        with path.open("rb") as fh:
            fh.seek(old, io.SEEK_SET)
            chunk = fh.read()
            new_offset = fh.tell()
        if not chunk:
            self.offsets[key] = new_offset
            return [], new_offset

        rows: list[dict[str, Any]] = []
        try:
            text = chunk.decode("utf-8", errors="ignore")
            if path.suffix.lower() == ".jsonl":
                for raw_line in text.splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        rows.append(obj)
            elif path.suffix.lower() == ".csv":
                if old == 0:
                    reader = csv.DictReader(io.StringIO(text))
                    rows.extend(dict(r) for r in reader)
                    if reader.fieldnames:
                        self._csv_headers[key] = [str(c) for c in reader.fieldnames]
                else:
                    header = self._load_csv_header(path)
                    lines = [ln for ln in text.splitlines() if ln.strip()]
                    if lines and header:
                        reader = csv.DictReader(io.StringIO("\n".join(lines)), fieldnames=header)
                        rows.extend(dict(r) for r in reader)
        except Exception as exc:
            print(f"[paper-live] warning: failed to parse {path}: {exc}")
            rows = []

        self.offsets[key] = new_offset
        return rows, new_offset

    def _append_records(
        self,
        *,
        old_len: int,
        session: MarketSession,
        event_row: dict[str, Any],
        p1_price: float | None,
        p2_price: float | None,
        close_pnl_estimate: float = 0.0,
    ) -> None:
        for rec in session.machine.audit_log[old_len:]:
            pnl_estimate = close_pnl_estimate if rec.action == "close_position" else 0.0
            self.trade_log_rows.append(
                {
                    "event_ts": str(event_row.get("timestamp_utc", "")),
                    "market_key": str(event_row.get("market_key", "")),
                    "action": rec.action,
                    "state_from": rec.from_state.value,
                    "state_to": rec.to_state.value,
                    "p1_price": "" if p1_price is None else round(float(p1_price), 8),
                    "p2_price": "" if p2_price is None else round(float(p2_price), 8),
                    "pnl_estimate": round(float(pnl_estimate), 10),
                    "reason_code": rec.reason_code,
                }
            )
            self.reason_counts[rec.reason_code] += 1
            if rec.reason_code == LEG1_OPENED and rec.action == "open_leg1":
                self.leg1_opened += 1
            if rec.reason_code == LEG2_HEDGE_FILLED and rec.action == "try_open_leg2":
                self.leg2_hedged += 1
            if rec.to_state == LeggingState.UNWIND:
                self.unwind_count += 1

    def _choose_leg1_side_and_prices(self, row: dict[str, Any]) -> tuple[str, float, float] | None:
        up = _safe_float(row.get("up_price"), default=None)
        down = _safe_float(row.get("down_price"), default=None)
        if up is None:
            up = _safe_float(row.get("yes_ask"), default=None)
        if down is None:
            down = _safe_float(row.get("no_ask"), default=None)
        if up is None or down is None:
            return None
        if up < down:
            return "up", float(up), float(down)
        if down < up:
            return "down", float(down), float(up)
        if self.rng.random() < 0.5:
            return "up", float(up), float(down)
        return "down", float(down), float(up)

    def _process_prices(self, price_rows: list[dict[str, Any]]) -> None:
        for row in price_rows:
            market_key = str(row.get("market_key", "")).strip()
            if not market_key:
                continue
            logical_ts = _derive_logical_ts(row, fallback=self.cycle_id)
            seconds_to_close = _derive_seconds_to_close(row)

            session = self.market_sessions.get(market_key)
            if session is None:
                chosen = self._choose_leg1_side_and_prices(row)
                if chosen is None:
                    continue
                side, p1_open, _p2_initial = chosen
                sm = LeggingStateMachine(config=self.config)
                session = MarketSession(machine=sm, leg1_side=side, leg1_open_price=p1_open)
                self.trades_attempted += 1
                old_len = len(sm.audit_log)
                opened = sm.open_leg1(p1_price=p1_open, seconds_to_close=seconds_to_close, logical_ts=logical_ts)
                self._append_records(
                    old_len=old_len,
                    session=session,
                    event_row=row,
                    p1_price=p1_open,
                    p2_price=None,
                )
                if opened:
                    self.market_sessions[market_key] = session
                continue

            up = _safe_float(row.get("up_price"), default=None)
            down = _safe_float(row.get("down_price"), default=None)
            if up is None:
                up = _safe_float(row.get("yes_ask"), default=None)
            if down is None:
                down = _safe_float(row.get("no_ask"), default=None)
            if up is None or down is None:
                continue

            if session.leg1_side == "up":
                p1_cur = float(up)
                p2_cur = float(down)
            else:
                p1_cur = float(down)
                p2_cur = float(up)

            old_len = len(session.machine.audit_log)
            session.machine.try_open_leg2(p2_price=p2_cur, logical_ts=logical_ts)
            self._append_records(
                old_len=old_len,
                session=session,
                event_row=row,
                p1_price=session.leg1_open_price,
                p2_price=p2_cur,
            )

            if session.machine.state == LeggingState.LEG1_OPEN:
                loss_bps = _calc_loss_bps(session.leg1_open_price, p1_cur)
                old_len = len(session.machine.audit_log)
                session.machine.unwind_on_loss(observed_loss_bps=loss_bps, logical_ts=logical_ts)
                self._append_records(
                    old_len=old_len,
                    session=session,
                    event_row=row,
                    p1_price=session.leg1_open_price,
                    p2_price=p2_cur,
                )

            if session.machine.state in {LeggingState.HEDGED, LeggingState.UNWIND}:
                state_before_close = session.machine.state
                if state_before_close == LeggingState.HEDGED:
                    close_pnl = _calc_hedged_pnl(self.config, session.leg1_open_price, p2_cur)
                else:
                    loss_bps = _calc_loss_bps(session.leg1_open_price, p1_cur)
                    close_pnl = _calc_unwind_pnl(self.config, loss_bps)

                old_len = len(session.machine.audit_log)
                session.machine.close_position(logical_ts=logical_ts)
                self._append_records(
                    old_len=old_len,
                    session=session,
                    event_row=row,
                    p1_price=session.leg1_open_price,
                    p2_price=p2_cur,
                    close_pnl_estimate=close_pnl,
                )

                if session.machine.state == LeggingState.CLOSED:
                    self.final_state_counts[session.machine.state.value] += 1
                    self.closed_trades += 1
                    self.pnl_total += close_pnl
                self.market_sessions.pop(market_key, None)

    def _build_metrics_snapshot(self, rows_read_cycle: dict[str, int]) -> dict[str, Any]:
        summary = {
            "trades_attempted": int(self.trades_attempted),
            "leg1_opened": int(self.leg1_opened),
            "leg2_hedged": int(self.leg2_hedged),
            "unwind_count": int(self.unwind_count),
            "pnl_total": float(self.pnl_total),
        }
        metrics = compute_metrics(
            summary=summary,
            trade_log_rows=self.trade_log_rows,
            reason_code_counts=dict(self.reason_counts),
        )
        snapshot = {
            "engine_status": "running",
            "cycle_id": self.cycle_id,
            "last_cycle_utc": _iso_utc_now(),
            "rows_read_cycle": rows_read_cycle,
            "active_markets": len(self.market_sessions),
            "closed_trades": int(self.closed_trades),
            "final_state_counts": dict(self.final_state_counts),
            "metrics": metrics,
        }
        return snapshot

    def _write_snapshot(self, snapshot: dict[str, Any]) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.metrics_file.with_suffix(self.metrics_file.suffix + ".tmp")
        tmp.write_text(json.dumps(snapshot, indent=2, ensure_ascii=True), encoding="utf-8")
        tmp.replace(self.metrics_file)
        self.last_snapshot = snapshot

    def _append_event_log(self, rows_read_cycle: dict[str, int], metrics: dict[str, Any]) -> None:
        line = (
            f"{_iso_utc_now()} cycle_id={self.cycle_id} rows={rows_read_cycle} "
            f"leg1_opened={metrics.get('leg1_opened', 0)} leg2_hedged={metrics.get('leg2_hedged', 0)} "
            f"unwind_count={metrics.get('unwind_count', 0)} pnl_total={metrics.get('pnl_total', 0.0)}"
        )
        with self.events_log_file.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(line + "\n")

    def _write_top_csv(self, snapshot: dict[str, Any]) -> None:
        metrics = snapshot.get("metrics", {})
        top_reason = "-"
        top_reason_codes = metrics.get("top_reason_codes", [])
        if top_reason_codes:
            top_reason = str(top_reason_codes[0][0])
        with self.top_file.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "as_of_utc",
                    "profile_id",
                    "hedge_success_rate",
                    "pnl_total",
                    "max_drawdown_pct",
                    "p99_loss",
                    "top_reason_code",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "as_of_utc": snapshot.get("last_cycle_utc", ""),
                    "profile_id": "paper_default",
                    "hedge_success_rate": metrics.get("hedge_success_rate", 0.0),
                    "pnl_total": metrics.get("pnl_total", 0.0),
                    "max_drawdown_pct": metrics.get("max_drawdown_pct", 0.0),
                    "p99_loss": metrics.get("p99_loss", 0.0),
                    "top_reason_code": top_reason,
                }
            )

    def run_one_cycle(self) -> dict[str, Any]:
        self.cycle_id += 1
        source_files = self._resolve_source_files()
        rows_read_cycle: dict[str, int] = {"prices": 0, "trades": 0, "orderbook": 0, "total": 0}

        price_rows: list[dict[str, Any]] = []
        for kind in ("trades", "prices", "orderbook"):
            for path in source_files[kind]:
                rows, _new_offset = self._read_new_rows(path)
                rows_read_cycle[kind] += len(rows)
                if kind == "prices":
                    price_rows.extend(rows)
        rows_read_cycle["total"] = rows_read_cycle["prices"] + rows_read_cycle["trades"] + rows_read_cycle["orderbook"]

        self._process_prices(price_rows)
        snapshot = self._build_metrics_snapshot(rows_read_cycle=rows_read_cycle)
        self._write_snapshot(snapshot)
        self._append_event_log(rows_read_cycle=rows_read_cycle, metrics=snapshot["metrics"])
        self._write_top_csv(snapshot)

        if self.checkpoint_store is not None and self.use_checkpoint:
            self.checkpoint_store.save(self.offsets)
        return snapshot

    def shutdown(self) -> None:
        snapshot = dict(self.last_snapshot) if self.last_snapshot else self._build_metrics_snapshot(
            rows_read_cycle={"prices": 0, "trades": 0, "orderbook": 0, "total": 0}
        )
        snapshot["engine_status"] = "stopped"
        snapshot["last_cycle_utc"] = _iso_utc_now()
        self._write_snapshot(snapshot)
        if self.checkpoint_store is not None and self.use_checkpoint:
            self.checkpoint_store.save(self.offsets)

    def run(self, *, runtime_sec: int = 0, max_cycles: int | None = None) -> dict[str, Any]:
        started = time.time()
        try:
            while True:
                cycle_start = time.time()
                snapshot = self.run_one_cycle()
                metrics = snapshot.get("metrics", {})
                print(
                    f"[paper-live] cycle_id={self.cycle_id} rows={snapshot.get('rows_read_cycle', {}).get('total', 0)} "
                    f"leg1_opened={metrics.get('leg1_opened', 0)} leg2_hedged={metrics.get('leg2_hedged', 0)} "
                    f"unwind_count={metrics.get('unwind_count', 0)} pnl_total={metrics.get('pnl_total', 0.0)}"
                )

                if max_cycles is not None and self.cycle_id >= int(max_cycles):
                    break
                if int(runtime_sec) > 0 and (time.time() - started) >= int(runtime_sec):
                    break
                elapsed = time.time() - cycle_start
                sleep_for = max(0.0, float(self.interval_sec) - elapsed)
                if sleep_for > 0:
                    time.sleep(sleep_for)
        except KeyboardInterrupt:
            print("[paper-live] interrupted by Ctrl+C")
        finally:
            self.shutdown()
        return self.last_snapshot

