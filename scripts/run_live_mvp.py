from __future__ import annotations

import argparse
import concurrent.futures
import csv
import io
import json
import re
import sys
import threading
import time
import urllib.parse
import zlib
from collections import Counter, deque
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.io.market_data_collector import MarketDataCollector, load_collector_config, normalize_levels
from src.execution.polymarket_auth import PolymarketAuthConfig
from src.execution.polymarket_live_executor import LiveOrderResult, PolymarketLiveExecutor

try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover - optional runtime dependency
    websocket = None


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


def _is_balance_allowance_error(message: str) -> bool:
    txt = str(message or "").strip().lower()
    if not txt:
        return False
    return ("not enough balance" in txt) or ("allowance" in txt and "not enough" in txt)


def _extract_balance_order_amount(message: str) -> tuple[float | None, float | None]:
    txt = str(message or "")
    low = txt.lower()
    try:
        bal_key = "balance:"
        ord_key = "order amount:"
        i = low.find(bal_key)
        j = low.find(ord_key)
        if i < 0 or j < 0:
            return None, None
        bal_txt = "".join(ch for ch in txt[i + len(bal_key) : j] if ch.isdigit() or ch == ".")
        ord_txt = "".join(ch for ch in txt[j + len(ord_key) :] if ch.isdigit() or ch == ".")
        bal = float(bal_txt) if bal_txt else None
        amt = float(ord_txt) if ord_txt else None
        return bal, amt
    except Exception:
        return None, None


def _extract_min_size_shares(message: str) -> float | None:
    txt = str(message or "")
    if not txt:
        return None
    match = re.search(r"lower than the minimum:\s*([0-9]+(?:\.[0-9]+)?)", txt, flags=re.IGNORECASE)
    if match is None:
        return None
    try:
        return float(match.group(1))
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class LiveMVPConfig:
    market_scope: str = "SOL5M"
    price_min: float = 0.05
    price_max: float = 0.95
    max_sum_ask: float = 0.99
    edge_high_max_sum_ask: float = 0.95
    edge_mid_max_sum_ask: float = 0.98
    edge_weak_max_sum_ask: float = 0.992
    target_profit_pct: float = 3.0
    target_profit_max_pct: float = 3.0
    entry_cutoff_sec: int = 75
    max_spread: float = 0.02
    min_depth_buffer_mult: float = 1.2
    max_unwind_loss_bps: float = 120.0
    stake_usd_per_leg: float = 1.0
    max_trades_per_market: int = 1
    leg2_timeout_ms: int = 2000
    leg2_timeout_high_ms: int = 3000
    leg2_timeout_mid_ms: int = 3000
    leg2_timeout_weak_ms: int = 10000
    mid_edge_stable_ticks: int = 2
    leg1_order_type: str = "FAK"
    leg2_order_type: str = "FAK"
    unwind_order_type: str = "FAK"
    maker_min_size_shares: float = 5.0
    dual_fok_enabled: bool = False
    dual_fok_timeout_ms: int = 1200
    preflight_on_start: bool = True
    max_daily_loss_usd: float = 3.0
    poll_interval_sec: float = 0.5
    api_book_depth: int = 10
    api_markets_count: int = 1
    api_market_slugs: tuple[str, ...] = ()
    api_market_urls: tuple[str, ...] = ()
    api_parallel_workers: int = 2
    api_max_markets_per_cycle: int = 4
    api_max_data_freshness_ms: int = 1500
    api_top_interval_ms: int = 100
    api_book_interval_ms: int = 250
    api_trades_interval_ms: int = 400
    api_request_timeout_ms: int = 350
    api_request_retries: int = 0
    api_request_backoff_ms: int = 0
    api_use_effective_quotes: bool = True
    api_skip_unchanged_book: bool = True
    api_backoff_on_error: bool = True
    api_max_rps_guard: float = 25.0
    api_ws_enabled: bool = False
    api_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    api_ws_shards: int = 1
    api_ws_reconnect_sec: float = 1.0
    api_ws_stale_sec: float = 1.0
    api_ws_fallback_rest: bool = True
    api_ws_ping_interval_sec: float = 10.0
    api_ws_ping_timeout_sec: float = 5.0
    api_prewarm_next_market: bool = True
    api_prewarm_sec: int = 45
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
        if not (
            0 < float(self.edge_high_max_sum_ask) <= float(self.edge_mid_max_sum_ask) <= float(self.edge_weak_max_sum_ask) <= 1
        ):
            raise ValueError("edge thresholds must satisfy 0 < high <= mid <= weak <= 1")
        if float(self.target_profit_pct) <= 0 or float(self.target_profit_pct) >= 100:
            raise ValueError("target_profit_pct must be in (0, 100)")
        if float(self.target_profit_max_pct) <= 0 or float(self.target_profit_max_pct) >= 100:
            raise ValueError("target_profit_max_pct must be in (0, 100)")
        if float(self.target_profit_max_pct) + EPS < float(self.target_profit_pct):
            raise ValueError("target_profit_max_pct must be >= target_profit_pct")
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
        if int(self.leg2_timeout_high_ms) <= 0 or int(self.leg2_timeout_mid_ms) <= 0 or int(self.leg2_timeout_weak_ms) <= 0:
            raise ValueError("all leg2 timeout values must be > 0")
        if int(self.mid_edge_stable_ticks) < 1:
            raise ValueError("mid_edge_stable_ticks must be >= 1")
        allowed_order_types = {"GTC", "FOK", "FAK"}
        if str(self.leg1_order_type).upper() not in allowed_order_types:
            raise ValueError("leg1_order_type must be one of GTC|FOK|FAK")
        if str(self.leg2_order_type).upper() not in allowed_order_types:
            raise ValueError("leg2_order_type must be one of GTC|FOK|FAK")
        if str(self.unwind_order_type).upper() not in allowed_order_types:
            raise ValueError("unwind_order_type must be one of GTC|FOK|FAK")
        if float(self.maker_min_size_shares) <= 0:
            raise ValueError("maker_min_size_shares must be > 0")
        if int(self.dual_fok_timeout_ms) <= 0:
            raise ValueError("dual_fok_timeout_ms must be > 0")
        if float(self.max_daily_loss_usd) <= 0:
            raise ValueError("max_daily_loss_usd must be > 0")
        if float(self.poll_interval_sec) <= 0:
            raise ValueError("poll_interval_sec must be > 0")
        if int(self.api_book_depth) < 1:
            raise ValueError("api_book_depth must be >= 1")
        if int(self.api_markets_count) < 1:
            raise ValueError("api_markets_count must be >= 1")
        if not isinstance(self.api_market_slugs, tuple):
            raise ValueError("api_market_slugs must be a tuple")
        if not isinstance(self.api_market_urls, tuple):
            raise ValueError("api_market_urls must be a tuple")
        if int(self.api_parallel_workers) < 1:
            raise ValueError("api_parallel_workers must be >= 1")
        if int(self.api_max_markets_per_cycle) < 1:
            raise ValueError("api_max_markets_per_cycle must be >= 1")
        if int(self.api_max_data_freshness_ms) < 100:
            raise ValueError("api_max_data_freshness_ms must be >= 100")
        if int(self.api_top_interval_ms) < 10:
            raise ValueError("api_top_interval_ms must be >= 10")
        if int(self.api_book_interval_ms) < int(self.api_top_interval_ms):
            raise ValueError("api_book_interval_ms must be >= api_top_interval_ms")
        if int(self.api_trades_interval_ms) < int(self.api_top_interval_ms):
            raise ValueError("api_trades_interval_ms must be >= api_top_interval_ms")
        if int(self.api_request_timeout_ms) < 50:
            raise ValueError("api_request_timeout_ms must be >= 50")
        if int(self.api_request_retries) < 0:
            raise ValueError("api_request_retries must be >= 0")
        if int(self.api_request_backoff_ms) < 0:
            raise ValueError("api_request_backoff_ms must be >= 0")
        if float(self.api_max_rps_guard) < 1.0:
            raise ValueError("api_max_rps_guard must be >= 1")
        if str(self.api_ws_url).strip() and not str(self.api_ws_url).strip().lower().startswith(("ws://", "wss://")):
            raise ValueError("api_ws_url must start with ws:// or wss://")
        if int(self.api_ws_shards) < 1:
            raise ValueError("api_ws_shards must be >= 1")
        if float(self.api_ws_reconnect_sec) <= 0:
            raise ValueError("api_ws_reconnect_sec must be > 0")
        if float(self.api_ws_stale_sec) <= 0:
            raise ValueError("api_ws_stale_sec must be > 0")
        if float(self.api_ws_ping_interval_sec) <= 0:
            raise ValueError("api_ws_ping_interval_sec must be > 0")
        if float(self.api_ws_ping_timeout_sec) <= 0:
            raise ValueError("api_ws_ping_timeout_sec must be > 0")
        if int(self.api_prewarm_sec) < 0:
            raise ValueError("api_prewarm_sec must be >= 0")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LiveMVPConfig":
        def _tuple_str_list(value: Any) -> tuple[str, ...]:
            if isinstance(value, list):
                return tuple(str(x).strip() for x in value if str(x).strip())
            if isinstance(value, tuple):
                return tuple(str(x).strip() for x in value if str(x).strip())
            if isinstance(value, str) and str(value).strip():
                return (str(value).strip(),)
            return ()

        return cls(
            market_scope=str(payload.get("market_scope", "SOL5M")).strip().upper(),
            price_min=float(payload.get("price_min", 0.05)),
            price_max=float(payload.get("price_max", 0.95)),
            max_sum_ask=float(payload.get("max_sum_ask", 0.99)),
            edge_high_max_sum_ask=float(payload.get("edge_high_max_sum_ask", 0.95)),
            edge_mid_max_sum_ask=float(payload.get("edge_mid_max_sum_ask", 0.98)),
            edge_weak_max_sum_ask=float(payload.get("edge_weak_max_sum_ask", 0.992)),
            target_profit_pct=float(payload.get("target_profit_pct", 3.0)),
            target_profit_max_pct=float(payload.get("target_profit_max_pct", payload.get("target_profit_pct", 3.0))),
            entry_cutoff_sec=int(payload.get("entry_cutoff_sec", 75)),
            max_spread=float(payload.get("max_spread", 0.02)),
            min_depth_buffer_mult=float(payload.get("min_depth_buffer_mult", 1.2)),
            max_unwind_loss_bps=float(payload.get("max_unwind_loss_bps", 120.0)),
            stake_usd_per_leg=float(payload.get("stake_usd_per_leg", 1.0)),
            max_trades_per_market=int(payload.get("max_trades_per_market", 1)),
            leg2_timeout_ms=int(payload.get("leg2_timeout_ms", 2000)),
            leg2_timeout_high_ms=int(payload.get("leg2_timeout_high_ms", 3000)),
            leg2_timeout_mid_ms=int(payload.get("leg2_timeout_mid_ms", 3000)),
            leg2_timeout_weak_ms=int(payload.get("leg2_timeout_weak_ms", 10000)),
            mid_edge_stable_ticks=int(payload.get("mid_edge_stable_ticks", 2)),
            leg1_order_type=str(payload.get("leg1_order_type", "FAK")).strip().upper(),
            leg2_order_type=str(payload.get("leg2_order_type", "FAK")).strip().upper(),
            unwind_order_type=str(payload.get("unwind_order_type", "FAK")).strip().upper(),
            maker_min_size_shares=float(payload.get("maker_min_size_shares", 5.0)),
            dual_fok_enabled=bool(payload.get("dual_fok_enabled", False)),
            dual_fok_timeout_ms=int(payload.get("dual_fok_timeout_ms", 1200)),
            preflight_on_start=bool(payload.get("preflight_on_start", True)),
            max_daily_loss_usd=float(payload.get("max_daily_loss_usd", 3.0)),
            poll_interval_sec=float(payload.get("poll_interval_sec", 0.5)),
            api_book_depth=int(payload.get("api_book_depth", 10)),
            api_markets_count=int(payload.get("api_markets_count", 1)),
            api_market_slugs=_tuple_str_list(payload.get("api_market_slugs", ())),
            api_market_urls=_tuple_str_list(payload.get("api_market_urls", ())),
            api_parallel_workers=int(payload.get("api_parallel_workers", 2)),
            api_max_markets_per_cycle=int(payload.get("api_max_markets_per_cycle", 4)),
            api_max_data_freshness_ms=int(payload.get("api_max_data_freshness_ms", 1500)),
            api_top_interval_ms=int(payload.get("api_top_interval_ms", 100)),
            api_book_interval_ms=int(payload.get("api_book_interval_ms", 250)),
            api_trades_interval_ms=int(payload.get("api_trades_interval_ms", 400)),
            api_request_timeout_ms=int(payload.get("api_request_timeout_ms", 350)),
            api_request_retries=int(payload.get("api_request_retries", 0)),
            api_request_backoff_ms=int(payload.get("api_request_backoff_ms", 0)),
            api_use_effective_quotes=bool(payload.get("api_use_effective_quotes", True)),
            api_skip_unchanged_book=bool(payload.get("api_skip_unchanged_book", True)),
            api_backoff_on_error=bool(payload.get("api_backoff_on_error", True)),
            api_max_rps_guard=float(payload.get("api_max_rps_guard", 25.0)),
            api_ws_enabled=bool(payload.get("api_ws_enabled", False)),
            api_ws_url=str(
                payload.get("api_ws_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
            ).strip(),
            api_ws_shards=int(payload.get("api_ws_shards", 1)),
            api_ws_reconnect_sec=float(payload.get("api_ws_reconnect_sec", 1.0)),
            api_ws_stale_sec=float(payload.get("api_ws_stale_sec", 1.0)),
            api_ws_fallback_rest=bool(payload.get("api_ws_fallback_rest", True)),
            api_ws_ping_interval_sec=float(payload.get("api_ws_ping_interval_sec", 10.0)),
            api_ws_ping_timeout_sec=float(payload.get("api_ws_ping_timeout_sec", 5.0)),
            api_prewarm_next_market=bool(payload.get("api_prewarm_next_market", True)),
            api_prewarm_sec=int(payload.get("api_prewarm_sec", 45)),
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
    leg2_target_max_ask: float
    sum_ask_entry: float
    phase_high_deadline_ms: int = 0
    phase_mid_deadline_ms: int = 0
    phase_weak_deadline_ms: int = 0
    edge_regime: str = "MID_EDGE"
    leg2_mode: str = "aggressive"
    leg1_token_id: str = ""
    leg2_token_id: str = ""
    quantity: float = 0.0
    next_hedge_retry_ms: int = 0
    order_id_leg1: str = ""
    order_id_leg2: str = ""
    fill_status_leg1: str = ""
    fill_status_leg2: str = ""


def load_live_mvp_config(path: str | Path) -> LiveMVPConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("live_mvp config must be a JSON object")
    return LiveMVPConfig.from_dict(payload)


class PolymarketMarketWsFeed:
    def __init__(
        self,
        *,
        ws_url: str,
        reconnect_sec: float = 1.0,
        ping_interval_sec: float = 15.0,
        ping_timeout_sec: float = 5.0,
    ) -> None:
        if websocket is None:
            raise RuntimeError("websocket_client_unavailable")
        self.ws_url = str(ws_url).strip()
        self.reconnect_sec = max(0.2, float(reconnect_sec))
        self.ping_interval_sec = max(1.0, float(ping_interval_sec))
        self.ping_timeout_sec = max(1.0, float(ping_timeout_sec))
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws_app: Any = None
        self._connected = False
        self._assets: list[str] = []
        self._last_message_ms = 0
        self._last_error = ""
        self._books_by_asset: dict[str, dict[str, Any]] = {}

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="polymarket-market-ws",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            ws_app = self._ws_app
        try:
            if ws_app is not None:
                ws_app.close()
        except Exception:
            pass
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self._thread = None
        with self._lock:
            self._connected = False
            self._ws_app = None

    def set_assets(self, asset_ids: list[str]) -> None:
        normalized = [str(x).strip() for x in asset_ids if str(x).strip()]
        normalized = list(dict.fromkeys(normalized))
        with self._lock:
            if normalized == self._assets:
                return
            self._assets = normalized
            ws_app = self._ws_app
            connected = self._connected
        if connected and ws_app is not None:
            self._send_subscribe(ws_app=ws_app)

    def get_pair_books(
        self,
        *,
        token_up: str,
        token_down: str,
        depth: int,
        last_pair_hash: str = "",
        skip_unchanged: bool = False,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, float | None, bool, str, str, bool]:
        now_ms = int(time.time() * 1000)
        with self._lock:
            up = self._books_by_asset.get(str(token_up))
            down = self._books_by_asset.get(str(token_down))
            connected = bool(self._connected)
            last_error = str(self._last_error or "")
        if up is None or down is None:
            return None, None, None, connected, last_error, "", False
        up_ts = int(up.get("timestamp_ms") or 0)
        down_ts = int(down.get("timestamp_ms") or 0)
        if up_ts <= 0 or down_ts <= 0:
            return None, None, None, connected, last_error, "", False
        age_sec = max(0.0, float(now_ms - min(up_ts, down_ts)) / 1000.0)
        pair_hash = self._pair_hash_from_books(up=up, down=down)
        if bool(skip_unchanged) and pair_hash and str(last_pair_hash or "").strip() == pair_hash:
            return None, None, age_sec, connected, last_error, pair_hash, True
        return (
            self._book_to_summary(asset_id=str(token_up), book=up, depth=depth),
            self._book_to_summary(asset_id=str(token_down), book=down, depth=depth),
            age_sec,
            connected,
            last_error,
            pair_hash,
            False,
        )

    def seed_pair_from_rest(
        self,
        *,
        token_up: str,
        token_down: str,
        up_book: dict[str, Any],
        down_book: dict[str, Any],
    ) -> None:
        self._seed_book_from_rest(asset_id=str(token_up), book=up_book)
        self._seed_book_from_rest(asset_id=str(token_down), book=down_book)

    def _run(self) -> None:
        assert websocket is not None
        while not self._stop_event.is_set():
            try:
                ws_app = websocket.WebSocketApp(  # type: ignore[attr-defined]
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                with self._lock:
                    self._ws_app = ws_app
                ws_app.run_forever(
                    ping_interval=float(self.ping_interval_sec),
                    ping_timeout=float(self.ping_timeout_sec),
                )
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._last_error = f"ws_run_exception:{exc}"
                    self._connected = False
                    self._ws_app = None
            if self._stop_event.is_set():
                break
            time.sleep(float(self.reconnect_sec))

    def _send_subscribe(self, *, ws_app: Any) -> None:
        with self._lock:
            assets = list(self._assets)
        if not assets:
            return
        payload = {
            "type": "market",
            "assets_ids": assets,
            "custom_feature_enabled": True,
        }
        try:
            ws_app.send(json.dumps(payload))
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self._last_error = f"ws_subscribe_error:{exc}"

    def _on_open(self, ws_app: Any) -> None:
        with self._lock:
            self._connected = True
            self._last_error = ""
        self._send_subscribe(ws_app=ws_app)

    def _on_close(self, _ws_app: Any, _code: Any, _msg: Any) -> None:
        with self._lock:
            self._connected = False
            self._ws_app = None

    def _on_error(self, _ws_app: Any, error: Any) -> None:
        with self._lock:
            self._connected = False
            self._last_error = f"ws_error:{error}"

    def _on_message(self, _ws_app: Any, message: str) -> None:
        try:
            payload = json.loads(message)
        except Exception:
            return
        now_ms = int(time.time() * 1000)
        events: list[dict[str, Any]] = []
        if isinstance(payload, list):
            events = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict):
            if "event_type" in payload:
                events = [payload]
            elif isinstance(payload.get("events"), list):
                events = [item for item in payload.get("events", []) if isinstance(item, dict)]
            else:
                events = [payload]
        if not events:
            return
        with self._lock:
            self._last_message_ms = now_ms
        for event in events:
            self._apply_event(event=event, now_ms=now_ms)

    def _apply_event(self, *, event: dict[str, Any], now_ms: int) -> None:
        event_type = str(event.get("event_type") or "").strip().lower()
        if not event_type:
            # Some market-channel messages come without `event_type`.
            # Infer type by payload shape.
            if isinstance(event.get("price_changes"), list):
                event_type = "price_change"
            else:
                asset_id_guess = str(event.get("asset_id") or "").strip()
                if asset_id_guess and isinstance(event.get("bids"), list) and isinstance(event.get("asks"), list):
                    event_type = "book"
                elif asset_id_guess and ("best_bid" in event or "best_ask" in event):
                    event_type = "best_bid_ask"
        if event_type == "book":
            asset_id = str(event.get("asset_id") or "").strip()
            if not asset_id:
                return
            bids_map = self._levels_to_map(event.get("bids"))
            asks_map = self._levels_to_map(event.get("asks"))
            min_size = _safe_float(event.get("min_order_size"), default=None)
            book_hash = str(event.get("hash") or "").strip()
            ts_ms = int(_safe_float(event.get("timestamp"), default=float(now_ms)) or now_ms)
            with self._lock:
                self._books_by_asset[asset_id] = {
                    "asset_id": asset_id,
                    "market": str(event.get("market") or "").strip(),
                    "bids": bids_map,
                    "asks": asks_map,
                    "min_order_size": min_size,
                    "hash": book_hash,
                    "timestamp_ms": ts_ms,
                }
            return

        if event_type == "price_change":
            price_changes = event.get("price_changes")
            if not isinstance(price_changes, list):
                return
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                asset_id = str(change.get("asset_id") or "").strip()
                side = str(change.get("side") or "").strip().upper()
                price = _safe_float(change.get("price"), default=None)
                size = _safe_float(change.get("size"), default=None)
                if not asset_id or side not in {"BUY", "SELL"} or price is None or size is None:
                    continue
                with self._lock:
                    book = self._books_by_asset.get(asset_id)
                    if book is None:
                        book = {
                            "asset_id": asset_id,
                            "market": str(event.get("market") or "").strip(),
                            "bids": {},
                            "asks": {},
                            "min_order_size": None,
                            "hash": "",
                            "timestamp_ms": int(now_ms),
                        }
                        self._books_by_asset[asset_id] = book
                    levels = book["bids"] if side == "BUY" else book["asks"]
                    if float(size) <= EPS:
                        levels.pop(float(price), None)
                    else:
                        levels[float(price)] = float(size)
                    book_hash = str(change.get("hash") or "").strip()
                    if book_hash:
                        book["hash"] = book_hash
                    book["timestamp_ms"] = int(_safe_float(event.get("timestamp"), default=float(now_ms)) or now_ms)
                    self._trim_levels_in_place(levels=book["bids"], reverse=True, limit=300)
                    self._trim_levels_in_place(levels=book["asks"], reverse=False, limit=300)
            return

        if event_type == "best_bid_ask":
            # Optional event with top-of-book only (no sizes). We keep current
            # depth maps untouched and only stamp freshness.
            asset_id = str(event.get("asset_id") or "").strip()
            if not asset_id:
                return
            with self._lock:
                book = self._books_by_asset.get(asset_id)
                if book is None:
                    self._books_by_asset[asset_id] = {
                        "asset_id": asset_id,
                        "market": str(event.get("market") or "").strip(),
                        "bids": {},
                        "asks": {},
                        "min_order_size": None,
                        "hash": "",
                        "timestamp_ms": int(_safe_float(event.get("timestamp"), default=float(now_ms)) or now_ms),
                    }
                else:
                    book["timestamp_ms"] = int(_safe_float(event.get("timestamp"), default=float(now_ms)) or now_ms)
            return

    def _levels_to_map(self, levels: Any) -> dict[float, float]:
        out: dict[float, float] = {}
        if not isinstance(levels, list):
            return out
        for item in levels:
            if not isinstance(item, dict):
                continue
            px = _safe_float(item.get("price"), default=None)
            sz = _safe_float(item.get("size"), default=None)
            if px is None or sz is None:
                continue
            if float(px) < 0.0 or float(px) > 1.0:
                continue
            if float(sz) <= EPS:
                continue
            out[float(px)] = float(sz)
        return out

    def _trim_levels_in_place(self, *, levels: dict[float, float], reverse: bool, limit: int) -> None:
        if len(levels) <= int(limit):
            return
        ordered = sorted(levels.items(), key=lambda x: float(x[0]), reverse=bool(reverse))
        keep = dict(ordered[: int(limit)])
        levels.clear()
        levels.update(keep)

    def _best_level(self, *, levels: dict[float, float], reverse: bool) -> tuple[float | None, float | None]:
        if not isinstance(levels, dict) or not levels:
            return None, None
        try:
            px = max(levels.keys()) if bool(reverse) else min(levels.keys())
            sz = _safe_float(levels.get(px), default=None)
            if sz is None:
                return None, None
            return float(px), float(sz)
        except Exception:
            return None, None

    def _pair_hash_from_books(self, *, up: dict[str, Any], down: dict[str, Any]) -> str:
        up_hash = str(up.get("hash") or "").strip()
        down_hash = str(down.get("hash") or "").strip()
        if up_hash and down_hash:
            return f"{up_hash}:{down_hash}"
        up_bids = up.get("bids") if isinstance(up.get("bids"), dict) else {}
        up_asks = up.get("asks") if isinstance(up.get("asks"), dict) else {}
        down_bids = down.get("bids") if isinstance(down.get("bids"), dict) else {}
        down_asks = down.get("asks") if isinstance(down.get("asks"), dict) else {}
        up_bid_px, up_bid_sz = self._best_level(levels=up_bids, reverse=True)
        up_ask_px, up_ask_sz = self._best_level(levels=up_asks, reverse=False)
        down_bid_px, down_bid_sz = self._best_level(levels=down_bids, reverse=True)
        down_ask_px, down_ask_sz = self._best_level(levels=down_asks, reverse=False)
        if (
            up_bid_px is not None
            and up_bid_sz is not None
            and up_ask_px is not None
            and up_ask_sz is not None
            and down_bid_px is not None
            and down_bid_sz is not None
            and down_ask_px is not None
            and down_ask_sz is not None
        ):
            return (
                f"pxsz:{up_bid_px:.6f}:{up_bid_sz:.8f}:{up_ask_px:.6f}:{up_ask_sz:.8f}:"
                f"{down_bid_px:.6f}:{down_bid_sz:.8f}:{down_ask_px:.6f}:{down_ask_sz:.8f}"
            )
        up_ts = str(int(up.get("timestamp_ms") or 0))
        down_ts = str(int(down.get("timestamp_ms") or 0))
        if up_ts != "0" and down_ts != "0":
            return f"ts:{up_ts}:{down_ts}"
        return ""

    def _book_to_summary(self, *, asset_id: str, book: dict[str, Any], depth: int) -> dict[str, Any]:
        bids_map = book.get("bids") if isinstance(book.get("bids"), dict) else {}
        asks_map = book.get("asks") if isinstance(book.get("asks"), dict) else {}
        bids_sorted = sorted(bids_map.items(), key=lambda x: float(x[0]), reverse=True)
        asks_sorted = sorted(asks_map.items(), key=lambda x: float(x[0]), reverse=False)
        depth_n = max(1, int(depth))
        bids = [{"price": f"{float(px):.6f}", "size": f"{float(sz):.8f}"} for px, sz in bids_sorted[:depth_n]]
        asks = [{"price": f"{float(px):.6f}", "size": f"{float(sz):.8f}"} for px, sz in asks_sorted[:depth_n]]
        return {
            "asset_id": asset_id,
            "market": str(book.get("market") or ""),
            "bids": bids,
            "asks": asks,
            "min_order_size": book.get("min_order_size"),
            "hash": str(book.get("hash") or ""),
            "timestamp": str(int(book.get("timestamp_ms") or 0)),
        }

    def _seed_book_from_rest(self, *, asset_id: str, book: dict[str, Any]) -> None:
        if not asset_id:
            return
        bids_map = self._levels_to_map(book.get("bids"))
        asks_map = self._levels_to_map(book.get("asks"))
        if not bids_map and not asks_map:
            return
        now_ms = int(time.time() * 1000)
        ts_ms = int(_safe_float(book.get("timestamp"), default=float(now_ms)) or now_ms)
        with self._lock:
            existing = self._books_by_asset.get(asset_id)
            market_txt = str(book.get("market") or "")
            if existing is not None and not market_txt:
                market_txt = str(existing.get("market") or "")
            self._books_by_asset[asset_id] = {
                "asset_id": asset_id,
                "market": market_txt,
                "bids": bids_map,
                "asks": asks_map,
                "min_order_size": _safe_float(book.get("min_order_size"), default=None),
                "hash": str(book.get("hash") or ""),
                "timestamp_ms": ts_ms,
            }


class PolymarketMarketWsShardPool:
    def __init__(
        self,
        *,
        ws_url: str,
        reconnect_sec: float = 1.0,
        ping_interval_sec: float = 10.0,
        ping_timeout_sec: float = 5.0,
        shards: int = 1,
    ) -> None:
        shard_count = max(1, int(shards))
        self._feeds = [
            PolymarketMarketWsFeed(
                ws_url=ws_url,
                reconnect_sec=reconnect_sec,
                ping_interval_sec=ping_interval_sec,
                ping_timeout_sec=ping_timeout_sec,
            )
            for _ in range(shard_count)
        ]
        self._lock = threading.Lock()
        self._pair_to_shard: dict[str, int] = {}
        self._assets_by_shard: list[set[str]] = [set() for _ in range(shard_count)]

    @property
    def shard_count(self) -> int:
        return int(len(self._feeds))

    def start(self) -> None:
        for feed in self._feeds:
            feed.start()

    def stop(self) -> None:
        for feed in self._feeds:
            feed.stop()

    def _pair_key(self, *, token_up: str, token_down: str) -> str:
        up = str(token_up or "").strip()
        down = str(token_down or "").strip()
        if up <= down:
            return f"{up}|{down}"
        return f"{down}|{up}"

    def _stable_shard_idx(self, *, pair_key: str) -> int:
        if not self._feeds:
            return 0
        checksum = zlib.crc32(str(pair_key).encode("utf-8")) & 0xFFFFFFFF
        return int(checksum % len(self._feeds))

    def set_pairs(self, pairs: list[tuple[str, str]]) -> None:
        with self._lock:
            next_pair_to_shard: dict[str, int] = {}
            assets_by_shard: list[set[str]] = [set() for _ in range(len(self._feeds))]
            for token_up, token_down in pairs:
                up = str(token_up or "").strip()
                down = str(token_down or "").strip()
                if not up or not down:
                    continue
                pair_key = self._pair_key(token_up=up, token_down=down)
                shard_idx = self._pair_to_shard.get(pair_key)
                if shard_idx is None:
                    shard_idx = self._stable_shard_idx(pair_key=pair_key)
                shard_idx = max(0, min(int(shard_idx), len(self._feeds) - 1))
                next_pair_to_shard[pair_key] = shard_idx
                assets_by_shard[shard_idx].add(up)
                assets_by_shard[shard_idx].add(down)
            self._pair_to_shard = next_pair_to_shard
            self._assets_by_shard = assets_by_shard
            assets_snapshot = [list(sorted(group)) for group in assets_by_shard]
        for idx, feed in enumerate(self._feeds):
            feed.set_assets(assets_snapshot[idx] if idx < len(assets_snapshot) else [])

    def get_pair_books(
        self,
        *,
        token_up: str,
        token_down: str,
        depth: int,
        last_pair_hash: str = "",
        skip_unchanged: bool = False,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, float | None, bool, str, str, bool]:
        pair_key = self._pair_key(token_up=token_up, token_down=token_down)
        with self._lock:
            shard_idx = self._pair_to_shard.get(pair_key)
        if shard_idx is None:
            shard_idx = self._stable_shard_idx(pair_key=pair_key)
        shard_idx = max(0, min(int(shard_idx), len(self._feeds) - 1))
        return self._feeds[shard_idx].get_pair_books(
            token_up=token_up,
            token_down=token_down,
            depth=depth,
            last_pair_hash=last_pair_hash,
            skip_unchanged=skip_unchanged,
        )

    def seed_pair_from_rest(
        self,
        *,
        token_up: str,
        token_down: str,
        up_book: dict[str, Any],
        down_book: dict[str, Any],
    ) -> None:
        pair_key = self._pair_key(token_up=token_up, token_down=token_down)
        with self._lock:
            shard_idx = self._pair_to_shard.get(pair_key)
        if shard_idx is None:
            shard_idx = self._stable_shard_idx(pair_key=pair_key)
        shard_idx = max(0, min(int(shard_idx), len(self._feeds) - 1))
        self._feeds[shard_idx].seed_pair_from_rest(
            token_up=token_up,
            token_down=token_down,
            up_book=up_book,
            down_book=down_book,
        )


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
        log_mode: str = "simple",
        heartbeat_sec: float = 5.0,
        show_orderbook: bool = False,
        terminal_dashboard: bool = False,
        dashboard_rows: int = 8,
        market_source: str = "file",
        collector_config_path: str = "configs/data_collection_sol5m.json",
        api_timeout_sec: float | None = None,
        api_retries: int | None = None,
        api_backoff_sec: float | None = None,
    ) -> None:
        self.cfg = cfg
        self.runtime_sec = max(0, int(runtime_sec))
        self.raw_dir = Path(raw_dir_override) if raw_dir_override else Path(cfg.raw_dir)
        self.report_file = Path(report_file_override) if report_file_override else Path(cfg.report_file)
        self.offsets: dict[str, int] = {}
        self.pending_by_market: dict[str, PendingTrade] = {}
        self.trades_closed_by_market: Counter[str] = Counter()
        self.trades_opened_by_market: Counter[str] = Counter()
        self._book_stable_ticks_by_market: Counter[str] = Counter()
        self._book_signature_by_market: dict[str, tuple[float, float, float, float]] = {}
        self.daily_pnl = 0.0
        self.rows_processed = 0
        self.actions_logged = 0
        self._offsets_bootstrapped = False
        self.enable_live = bool(enable_live)
        self.dry_run = bool(dry_run)
        self.executor = executor
        self.kill_switch_triggered = False
        self.kill_switch_reason = ""
        self.log_mode = str(log_mode or "simple").strip().lower()
        if self.log_mode not in {"simple", "verbose"}:
            self.log_mode = "simple"
        self.heartbeat_sec = max(1.0, float(heartbeat_sec))
        self.show_orderbook = bool(show_orderbook)
        self.terminal_dashboard = bool(terminal_dashboard)
        self.dashboard_rows = max(3, int(dashboard_rows))
        self._dashboard_events: list[dict[str, str]] = []
        self._last_heartbeat_at = 0.0
        self._last_skip_reason = ""
        self._last_skip_logged_ms = 0
        self._last_api_warning = ""
        self._last_api_warning_ms = 0
        self._last_book_snapshot: dict[str, Any] | None = None
        self._active_feed_source: str = ""
        self._last_loop_cycle_ms: float = 0.0
        self._last_data_freshness_ms: float = 0.0
        self._last_decision_latency_ms: float = 0.0
        self._current_decision_latency_ms: float = 0.0
        self._api_events_by_endpoint: dict[str, deque[tuple[int, float, bool]]] = {
            "top": deque(),
            "book": deque(),
            "trades": deque(),
            "ws": deque(),
        }
        self._suppress_api_metrics = False
        self._api_skip_unchanged_events: deque[int] = deque()
        self._api_global_requests_1s: deque[int] = deque()
        self.market_source = str(market_source or "file").strip().lower()
        if self.market_source not in {"file", "api"}:
            self.market_source = "file"
        self.collector_config_path = str(collector_config_path or "configs/data_collection_sol5m.json")
        self.api_timeout_sec = None if api_timeout_sec is None else max(0.2, float(api_timeout_sec))
        self.api_retries = None if api_retries is None else max(0, int(api_retries))
        self.api_backoff_sec = None if api_backoff_sec is None else max(0.0, float(api_backoff_sec))
        self._last_api_book_pair_hash_by_market: dict[str, str] = {}
        self._api_market_cache_by_slug: dict[str, tuple[str, str, str, str]] = {}
        self._api_market_schedule_ms: dict[str, dict[str, int]] = {}
        self._api_market_scan_cursor: int = 0
        self._last_snapshot_hash_by_market: dict[str, str] = {}
        self._api_endpoint_backoff_ms: dict[str, int] = {"top": 0, "book": 0, "trades": 0}
        self._api_endpoint_error_streak: dict[str, int] = {"top": 0, "book": 0, "trades": 0}
        self._api_last_error_by_endpoint: dict[str, str] = {"top": "", "book": "", "trades": ""}
        self._api_midpoint_by_market: dict[str, tuple[float | None, float | None]] = {}
        self._seen_trades_by_market: dict[str, set[str]] = {}
        self._seen_trades_queue_by_market: dict[str, deque[str]] = {}
        self._report_rows_buffer: list[dict[str, Any]] = []
        self._report_buffer_max_rows = 64
        self._report_buffer_flush_sec = 0.20
        self._report_last_flush_at = time.time()
        self.api_collector: MarketDataCollector | None = None
        self._api_pool: concurrent.futures.ThreadPoolExecutor | None = None
        self._api_trades_future: concurrent.futures.Future[str] | None = None
        self._api_ws_feed: PolymarketMarketWsShardPool | None = None
        if self.market_source == "api":
            self.api_collector = self._build_api_collector(config_path=self.collector_config_path)
            self._api_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=max(1, int(self.cfg.api_parallel_workers)),
                thread_name_prefix="live-mvp-api",
            )
            if bool(self.cfg.api_ws_enabled):
                try:
                    self._api_ws_feed = PolymarketMarketWsShardPool(
                        ws_url=str(self.cfg.api_ws_url),
                        reconnect_sec=float(self.cfg.api_ws_reconnect_sec),
                        ping_interval_sec=float(self.cfg.api_ws_ping_interval_sec),
                        ping_timeout_sec=float(self.cfg.api_ws_ping_timeout_sec),
                        shards=max(1, int(self.cfg.api_ws_shards)),
                    )
                    self._api_ws_feed.start()
                except Exception as exc:  # noqa: BLE001
                    self._api_ws_feed = None
                    self._warn_api_throttled(f"ws_disabled:{exc}")
        self._init_report_file()

    def _resolve_unwind_quantity(self, *, pending: PendingTrade) -> float:
        qty = float(pending.quantity)
        if not self._live_enabled() or self.executor is None:
            return qty
        available: float | None = None
        if pending.leg1_token_id:
            try:
                available = float(self.executor.get_token_available_balance(pending.leg1_token_id))
            except Exception:
                available = None
        order_id = str(pending.order_id_leg1 or "").strip()
        matched: float | None = None
        if order_id:
            try:
                status_payload = self.executor.get_order_status(order_id)
            except Exception:
                status_payload = {}
            matched = _safe_float(
                status_payload.get("size_matched") or status_payload.get("filled_size"),
                default=None,
            )
        if matched is not None and float(matched) >= 0.0:
            qty = min(qty, float(matched))
        if available is not None:
            # Safety haircut to avoid fee/precision mismatch causing balance errors.
            qty = min(qty, max(0.0, float(available) * 0.998))
        if not order_id:
            return qty
        return qty

    def _retry_unwind_on_balance_error(
        self,
        *,
        pending: PendingTrade,
        unwind_price_hint: float,
        leg2_ask: float,
        leg2_target: float,
        context_note: str,
    ) -> LiveOrderResult:
        if self.executor is None:
            return LiveOrderResult(
                success=False,
                order_id="",
                status="error",
                filled_size=0.0,
                avg_price=0.0,
                latency_ms=0,
                error="executor_missing",
            )
        deadline = time.time() + 8.0
        last_error = "unwind_balance_retry_exhausted"
        retry_qty_cap: float | None = None
        while time.time() < deadline:
            time.sleep(0.8)
            unwind_qty = self._resolve_unwind_quantity(pending=pending)
            if retry_qty_cap is not None:
                unwind_qty = min(unwind_qty, float(retry_qty_cap))
            if unwind_qty <= EPS:
                last_error = "inventory_unavailable_during_retry"
                continue
            unwind_result = self.executor.unwind_leg1(
                token_id=pending.leg1_token_id,
                price=float(unwind_price_hint),
                size=float(unwind_qty),
                order_type=str(self.cfg.unwind_order_type),
                timeout_ms=int(self.cfg.leg2_timeout_ms),
            )
            if unwind_result.success:
                return unwind_result
            last_error = str(unwind_result.error or unwind_result.status or "unwind_retry_failed")
            if not _is_balance_allowance_error(last_error):
                return unwind_result
            bal_raw, ord_raw = _extract_balance_order_amount(last_error)
            if bal_raw is not None and ord_raw is not None and ord_raw > EPS and bal_raw > EPS:
                ratio = max(0.0, min(1.0, float(bal_raw) / float(ord_raw)))
                adjusted_qty = max(EPS, float(unwind_qty) * ratio * 0.995)
                retry_qty_cap = adjusted_qty if retry_qty_cap is None else min(float(retry_qty_cap), adjusted_qty)
            self._append_action(
                ts_utc=_iso_utc_now(),
                market_key=pending.market_key,
                action="retry_unwind_leg1",
                status="retrying",
                reason_code="unwind_retry_balance_wait",
                leg1_side=pending.leg1_side,
                leg1_price=pending.leg1_ask,
                unwind_price=float(unwind_price_hint),
                order_id_leg1=pending.order_id_leg1,
                order_id_leg2=pending.order_id_leg2,
                fill_status_leg1=pending.fill_status_leg1 or "submitted",
                fill_status_leg2=pending.fill_status_leg2,
                unwind_triggered=True,
                note=(
                    f"{context_note} leg2_ask={float(leg2_ask):.6f} target_b<={float(leg2_target):.6f}; "
                    f"retry_error={last_error} retry_qty={float(unwind_qty):.6f}"
                ),
            )
        return LiveOrderResult(
            success=False,
            order_id="",
            status="error",
            filled_size=0.0,
            avg_price=0.0,
            latency_ms=0,
            error=last_error,
        )

    @staticmethod
    def _clip_text(value: Any, max_len: int) -> str:
        txt = str(value or "")
        if len(txt) <= int(max_len):
            return txt
        if int(max_len) <= 3:
            return txt[:max_len]
        return f"{txt[: max_len - 3]}..."

    def _record_dashboard_event(
        self,
        *,
        ts_utc: str,
        market_key: str,
        action: str,
        status: str,
        reason_code: str,
        leg1_side: str,
        sum_ask: float | None,
        pnl_delta: float,
    ) -> None:
        if not self.terminal_dashboard:
            return
        watched_actions = {
            "try_leg1",
            "enter_leg1",
            "try_hedge",
            "hedge_leg2",
            "unwind_leg1",
            "retry_unwind_leg1",
            "skip_entry",
            "critical_stop",
        }
        if action not in watched_actions:
            return

        action_to_result = {
            "try_leg1": "TENTANDO",
            "enter_leg1": "ENTROU_A",
            "try_hedge": "HEDGE_TENTANDO",
            "hedge_leg2": "HEDGE_OK",
            "unwind_leg1": "UNWIND",
            "retry_unwind_leg1": "RETRY_UNWIND",
            "skip_entry": "BLOQUEADA",
            "critical_stop": "KILL_SWITCH",
        }
        self._dashboard_events.append(
            {
                "ts": str(ts_utc or "")[11:19],
                "market": self._clip_text(market_key, 24),
                "result": action_to_result.get(action, action.upper()),
                "side": str(leg1_side or "-"),
                "sum_ask": "-" if sum_ask is None else f"{float(sum_ask):.4f}",
                "reason": self._clip_text(reason_code or "-", 30),
                "status": self._clip_text(status or "-", 12),
                "pnl_delta": f"{float(pnl_delta):+.4f}",
            }
        )
        if len(self._dashboard_events) > 400:
            self._dashboard_events = self._dashboard_events[-400:]

    def _print_terminal_dashboard(self) -> None:
        now_txt = _iso_utc_now()
        closed = int(sum(self.trades_closed_by_market.values()))
        opened = int(sum(self.trades_opened_by_market.values()))
        print(f"{now_txt} |================ LIVE MVP DASHBOARD ================")
        print(f"| metric         | value                                   |")
        print(f"| processed      | {self.rows_processed:<39} |")
        print(f"| abertas        | {len(self.pending_by_market):<39} |")
        print(f"| fechadas       | {closed:<39} |")
        print(f"| abertas_total  | {opened:<39} |")
        print(f"| pnl_dia_usd    | {self.daily_pnl:<39.6f} |")
        print(f"| kill_switch    | {str(bool(self.kill_switch_triggered)):<39} |")

        snap = self._last_book_snapshot
        if snap is None:
            print(f"| book           | sem_dados                               |")
        else:
            yes_bid = _safe_float(snap.get("yes_bid"), default=None)
            no_bid = _safe_float(snap.get("no_bid"), default=None)
            yes_bid_txt = "-" if yes_bid is None else f"{float(yes_bid):.4f}"
            no_bid_txt = "-" if no_bid is None else f"{float(no_bid):.4f}"
            line = (
                f"mkt={snap.get('market_key')} "
                f"yes(b={yes_bid_txt},a={float(snap.get('yes_ask', 0.0)):.4f}) "
                f"no(b={no_bid_txt},a={float(snap.get('no_ask', 0.0)):.4f}) "
                f"sum={float(snap.get('sum_ask', 0.0)):.4f} "
                f"tte={int(snap.get('seconds_to_close', 0))}s"
            )
            print(f"| book           | {self._clip_text(line, 39):<39} |")

        print("|")
        print("| ts       | market                   | resultado     | side | sum_ask | reason                        | status       | pnl_d |")
        events = self._dashboard_events[-self.dashboard_rows :]
        if not events:
            print("| -        | -                        | -             | -    | -       | -                             | -            | -     |")
            return
        for event in events:
            print(
                f"| {event['ts']:<8} | {event['market']:<24} | {event['result']:<13} | "
                f"{event['side']:<4} | {event['sum_ask']:<7} | {event['reason']:<29} | "
                f"{event['status']:<12} | {event['pnl_delta']:<5} |"
            )

    def _emit_console_action(
        self,
        *,
        ts_utc: str,
        market_key: str,
        action: str,
        reason_code: str,
        leg1_side: str,
        leg1_price: float | None,
        leg2_price: float | None,
        unwind_price: float | None,
        order_id_leg1: str,
        order_id_leg2: str,
        note: str,
    ) -> None:
        if self.log_mode != "simple":
            return
        if self.terminal_dashboard:
            return
        if action == "skip_entry":
            now_ms = int(time.time() * 1000)
            if reason_code != self._last_skip_reason or (now_ms - self._last_skip_logged_ms) >= int(self.heartbeat_sec * 1000):
                note_txt = ""
                if str(reason_code) == "blocked_target_profit_floor" and str(note or "").strip():
                    note_txt = f" note={note}"
                print(f"{ts_utc} | nao_entrou motivo={reason_code} market={market_key}{note_txt}")
                self._last_skip_reason = reason_code
                self._last_skip_logged_ms = now_ms
            return
        if action == "enter_leg1":
            target_txt = ""
            if leg2_price is not None:
                target_txt = f" alvo_perna_b<={leg2_price:.4f}"
            print(
                f"{ts_utc} | encontrou_sinal comprou_perna_a side={leg1_side} price={leg1_price} "
                f"order_a={order_id_leg1 or 'simulado'}{target_txt} market={market_key}"
            )
            return
        if action == "try_leg1":
            print(
                f"{ts_utc} | encontrou_sinal tentando_comprar_perna_a side={leg1_side} "
                f"price={leg1_price} market={market_key}"
            )
            return
        if action == "try_hedge":
            reason_txt = f" motivo={reason_code}" if str(reason_code or "").strip() else ""
            note_txt = ""
            if str(reason_code) in {"hedge_passive_post_failed", "hedge_retry_pending"} and str(note or "").strip():
                note_txt = f" note={note}"
            print(
                f"{ts_utc} | tentando_hedge_perna_b price_b={leg2_price} "
                f"order_a={order_id_leg1 or '-'}{reason_txt}{note_txt} market={market_key}"
            )
            return
        if action == "hedge_leg2":
            print(
                f"{ts_utc} | hedge_perna_b_ok price_b={leg2_price} order_a={order_id_leg1 or '-'} "
                f"order_b={order_id_leg2 or '-'} market={market_key}"
            )
            return
        if action == "unwind_leg1":
            print(
                f"{ts_utc} | hedge_falhou vendeu_perna_a price_saida={unwind_price} "
                f"order_a={order_id_leg1 or '-'} order_b={order_id_leg2 or '-'} motivo={reason_code} market={market_key}"
            )
            return
        if action == "retry_unwind_leg1":
            print(
                f"{ts_utc} | tentando_vender_perna_a_retry price_saida={unwind_price} "
                f"order_a={order_id_leg1 or '-'} motivo={reason_code} market={market_key}"
            )
            return
        if action == "critical_stop":
            print(f"{ts_utc} | ERRO_CRITICO kill_switch motivo={reason_code} note={note}")

    def _maybe_print_heartbeat(self) -> None:
        if self.log_mode != "simple":
            return
        now = time.time()
        if (now - self._last_heartbeat_at) < float(self.heartbeat_sec):
            return
        self._last_heartbeat_at = now
        if self.terminal_dashboard:
            self._print_terminal_dashboard()
            return
        perf = self._perf_snapshot()
        print(
            f"{_iso_utc_now()} | buscando_sinal processed={self.rows_processed} "
            f"abertas={len(self.pending_by_market)} fechadas={sum(self.trades_closed_by_market.values())} "
            f"pnl={self.daily_pnl:.6f} "
            f"loop_cycle_ms={perf['loop_cycle_ms']:.1f} "
            f"data_freshness_ms={perf['data_freshness_ms']:.1f} "
            f"decision_latency_ms={perf['decision_latency_ms']:.1f} "
            f"p95_api_latency_ms={perf['p95_api_latency_ms']:.1f} "
            f"errors_last_1m={int(perf['errors_last_1m'])} "
            f"requests_last_1m={int(perf['requests_last_1m'])}"
        )
        if not self.show_orderbook:
            return
        snap = self._last_book_snapshot
        if snap is None:
            print(f"{_iso_utc_now()} | book sem_dados")
            return
        yes_bid = _safe_float(snap.get("yes_bid"), default=None)
        no_bid = _safe_float(snap.get("no_bid"), default=None)
        yes_bid_txt = "-" if yes_bid is None else f"{float(yes_bid):.4f}"
        no_bid_txt = "-" if no_bid is None else f"{float(no_bid):.4f}"
        print(
            f"{_iso_utc_now()} | orderbook market={snap.get('market_key')} "
            f"yes(bid={yes_bid_txt},ask={float(snap.get('yes_ask', 0.0)):.4f},ask_sz={float(snap.get('yes_ask_size', 0.0)):.2f}) "
            f"no(bid={no_bid_txt},ask={float(snap.get('no_ask', 0.0)):.4f},ask_sz={float(snap.get('no_ask_size', 0.0)):.2f}) "
            f"sum_ask={float(snap.get('sum_ask', 0.0)):.4f} tte={int(snap.get('seconds_to_close', 0))}s"
        )

    def _report_fieldnames(self) -> list[str]:
        return [
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
            "feed_source",
            "loop_cycle_ms",
            "data_freshness_ms",
            "decision_latency_ms",
            "p95_api_latency_ms",
            "errors_last_1m",
            "requests_last_1m",
            "skip_unchanged_rate",
            "note",
        ]

    def _init_report_file(self) -> None:
        self.report_file.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = self._report_fieldnames()
        if self.report_file.exists() and self.report_file.stat().st_size > 0:
            try:
                with self.report_file.open("r", encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh)
                    current = [str(x) for x in (reader.fieldnames or [])]
                    if "feed_source" in current:
                        return
                    old_rows = list(reader)
                with self.report_file.open("w", encoding="utf-8", newline="") as fh:
                    writer = csv.DictWriter(fh, fieldnames=fieldnames)
                    writer.writeheader()
                    for old in old_rows:
                        row = {k: old.get(k, "") for k in fieldnames}
                        row["feed_source"] = old.get("feed_source", "")
                        writer.writerow(row)
                return
            except Exception:
                pass
        with self.report_file.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()

    def _flush_report_buffer_if_needed(self, *, force: bool = False) -> None:
        if not self._report_rows_buffer:
            return
        now = time.time()
        if not force:
            if len(self._report_rows_buffer) < int(self._report_buffer_max_rows):
                if (now - float(self._report_last_flush_at)) < float(self._report_buffer_flush_sec):
                    return
        with self.report_file.open("a", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=self._report_fieldnames(),
            )
            for row in self._report_rows_buffer:
                writer.writerow(row)
        self._report_rows_buffer.clear()
        self._report_last_flush_at = now

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
        feed_source: str | None = None,
        loop_cycle_ms: float | None = None,
        data_freshness_ms: float | None = None,
        decision_latency_ms: float | None = None,
        p95_api_latency_ms: float | None = None,
        errors_last_1m: float | None = None,
        requests_last_1m: float | None = None,
        skip_unchanged_rate: float | None = None,
        note: str = "",
    ) -> None:
        self.report_file.parent.mkdir(parents=True, exist_ok=True)
        feed_source_txt = str(feed_source if feed_source is not None else self._active_feed_source or "").strip()
        perf = self._perf_snapshot()
        loop_cycle_val = float(perf["loop_cycle_ms"] if loop_cycle_ms is None else loop_cycle_ms)
        data_freshness_val = float(perf["data_freshness_ms"] if data_freshness_ms is None else data_freshness_ms)
        decision_latency_val = float(
            perf["decision_latency_ms"] if decision_latency_ms is None else decision_latency_ms
        )
        p95_api_latency_val = float(
            perf["p95_api_latency_ms"] if p95_api_latency_ms is None else p95_api_latency_ms
        )
        errors_1m_val = float(perf["errors_last_1m"] if errors_last_1m is None else errors_last_1m)
        requests_1m_val = float(perf["requests_last_1m"] if requests_last_1m is None else requests_last_1m)
        skip_unchanged_val = float(
            perf["skip_unchanged_rate"] if skip_unchanged_rate is None else skip_unchanged_rate
        )
        self._report_rows_buffer.append(
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
                "feed_source": feed_source_txt,
                "loop_cycle_ms": round(loop_cycle_val, 3),
                "data_freshness_ms": round(data_freshness_val, 3),
                "decision_latency_ms": round(decision_latency_val, 3),
                "p95_api_latency_ms": round(p95_api_latency_val, 3),
                "errors_last_1m": int(max(0.0, errors_1m_val)),
                "requests_last_1m": int(max(0.0, requests_1m_val)),
                "skip_unchanged_rate": round(max(0.0, min(1.0, skip_unchanged_val)), 6),
                "note": note,
            }
        )
        self._flush_report_buffer_if_needed(force=False)
        self.actions_logged += 1
        self._record_dashboard_event(
            ts_utc=ts_utc,
            market_key=market_key,
            action=action,
            status=status,
            reason_code=reason_code,
            leg1_side=leg1_side,
            sum_ask=sum_ask,
            pnl_delta=pnl_delta,
        )
        self._emit_console_action(
            ts_utc=ts_utc,
            market_key=market_key,
            action=action,
            reason_code=reason_code,
            leg1_side=leg1_side,
            leg1_price=leg1_price,
            leg2_price=leg2_price,
            unwind_price=unwind_price,
            order_id_leg1=order_id_leg1,
            order_id_leg2=order_id_leg2,
            note=note,
        )

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

    def _build_api_collector(self, *, config_path: str) -> MarketDataCollector:
        collector_cfg = load_collector_config(config_path)
        cfg_timeout_sec = max(0.05, float(self.cfg.api_request_timeout_ms) / 1000.0)
        cfg_backoff_sec = max(0.0, float(self.cfg.api_request_backoff_ms) / 1000.0)
        cfg_retries = max(0, int(self.cfg.api_request_retries))
        if self.api_timeout_sec is not None or self.api_retries is not None or self.api_backoff_sec is not None:
            request_cfg = replace(
                collector_cfg.request,
                timeout_sec=(
                    float(self.api_timeout_sec)
                    if self.api_timeout_sec is not None
                    else cfg_timeout_sec
                ),
                retries=int(self.api_retries) if self.api_retries is not None else collector_cfg.request.retries,
                backoff_sec=(
                    float(self.api_backoff_sec) if self.api_backoff_sec is not None else collector_cfg.request.backoff_sec
                ),
            )
            collector_cfg = replace(collector_cfg, request=request_cfg)
        else:
            collector_cfg = replace(
                collector_cfg,
                request=replace(
                    collector_cfg.request,
                    timeout_sec=cfg_timeout_sec,
                    retries=cfg_retries,
                    backoff_sec=cfg_backoff_sec,
                ),
            )
        return MarketDataCollector(
            config=collector_cfg,
            raw_dir=self.raw_dir,
            interval_sec=float(self.cfg.poll_interval_sec),
            runtime_sec=0,
            book_depth=max(1, int(self.cfg.api_book_depth)),
            trade_limit=50,
            collect_orderbook=False,
            collect_trades=False,
            checkpoint_file=None,
            last_closed_file=None,
        )

    def _warn_api_throttled(self, message: str) -> None:
        now_ms = int(time.time() * 1000)
        if message != self._last_api_warning or (now_ms - self._last_api_warning_ms) >= int(self.heartbeat_sec * 1000):
            print(f"{_iso_utc_now()} | api_warning {message}")
            self._last_api_warning = message
            self._last_api_warning_ms = now_ms

    def _trim_recent_events(self, *, queue: deque[Any], now_ms: int, window_ms: int) -> None:
        while queue and (now_ms - int(queue[0][0] if isinstance(queue[0], tuple) else queue[0])) > int(window_ms):
            queue.popleft()

    def _record_api_event(self, *, endpoint: str, latency_ms: float, ok: bool) -> None:
        if bool(self._suppress_api_metrics):
            return
        endpoint_key = str(endpoint or "").strip().lower() or "book"
        now_ms = int(time.time() * 1000)
        queue = self._api_events_by_endpoint.setdefault(endpoint_key, deque())
        queue.append((now_ms, float(max(0.0, latency_ms)), bool(ok)))
        self._trim_recent_events(queue=queue, now_ms=now_ms, window_ms=60_000)
        self._api_global_requests_1s.append(now_ms)
        self._trim_recent_events(queue=self._api_global_requests_1s, now_ms=now_ms, window_ms=1_000)

    def _record_skip_unchanged(self) -> None:
        now_ms = int(time.time() * 1000)
        self._api_skip_unchanged_events.append(now_ms)
        self._trim_recent_events(queue=self._api_skip_unchanged_events, now_ms=now_ms, window_ms=60_000)

    @staticmethod
    def _schedule_key_for_endpoint(endpoint: str) -> str:
        key = str(endpoint or "").strip().lower()
        if key == "top":
            return "next_top_ms"
        if key == "trades":
            return "next_trades_ms"
        return "next_book_ms"

    def _base_interval_ms_for_endpoint(self, endpoint: str) -> int:
        key = str(endpoint or "").strip().lower()
        if key == "top":
            return max(10, int(self.cfg.api_top_interval_ms))
        if key == "trades":
            return max(10, int(self.cfg.api_trades_interval_ms))
        return max(10, int(self.cfg.api_book_interval_ms))

    def _effective_interval_ms_for_endpoint(self, endpoint: str) -> int:
        key = str(endpoint or "").strip().lower()
        base = self._base_interval_ms_for_endpoint(key)
        backoff = int(max(0, int(self._api_endpoint_backoff_ms.get(key, 0))))
        return int(base + backoff)

    def _market_schedule(self, *, market_key: str, now_ms: int) -> dict[str, int]:
        schedule = self._api_market_schedule_ms.get(str(market_key))
        if schedule is None:
            seed = int(zlib.crc32(str(market_key).encode("utf-8")) & 0xFFFFFFFF)
            top_i = max(10, self._base_interval_ms_for_endpoint("top"))
            book_i = max(10, self._base_interval_ms_for_endpoint("book"))
            trades_i = max(10, self._base_interval_ms_for_endpoint("trades"))
            schedule = {
                "next_top_ms": int(now_ms + (seed % top_i)),
                "next_book_ms": int(now_ms + (seed % book_i)),
                "next_trades_ms": int(now_ms + (seed % trades_i)),
            }
            self._api_market_schedule_ms[str(market_key)] = schedule
        return schedule

    def _is_endpoint_due(self, *, market_key: str, endpoint: str, now_ms: int) -> bool:
        schedule = self._market_schedule(market_key=market_key, now_ms=now_ms)
        key = self._schedule_key_for_endpoint(endpoint)
        return int(now_ms) >= int(schedule.get(key, 0))

    def _schedule_market_endpoint(self, *, market_key: str, endpoint: str, now_ms: int) -> None:
        schedule = self._market_schedule(market_key=market_key, now_ms=now_ms)
        key = self._schedule_key_for_endpoint(endpoint)
        interval = self._effective_interval_ms_for_endpoint(endpoint)
        current_next = int(schedule.get(key, now_ms))
        schedule[key] = int(max(now_ms + 1, current_next + interval))

    def _seconds_to_close_now(self, *, market_key: str, now_utc: datetime) -> int:
        if "_" not in str(market_key):
            return 86_400
        close_raw = str(market_key).split("_", 1)[1]
        close_dt = _parse_utc(close_raw)
        if close_dt is None:
            return 86_400
        return max(0, int((close_dt - now_utc).total_seconds()))

    def _select_markets_for_cycle(
        self,
        *,
        resolved_markets: list[tuple[str, str, str, str]],
        now_utc: datetime,
        now_ms: int,
    ) -> list[tuple[str, str, str, str, bool, bool, bool]]:
        due_rows: list[dict[str, Any]] = []
        for market_key, token_up, token_down, condition_id in resolved_markets:
            due_top = self._is_endpoint_due(market_key=market_key, endpoint="top", now_ms=now_ms)
            due_book = self._is_endpoint_due(market_key=market_key, endpoint="book", now_ms=now_ms)
            due_trades = self._is_endpoint_due(market_key=market_key, endpoint="trades", now_ms=now_ms)
            if not (due_top or due_book or due_trades):
                continue
            sec_to_close = self._seconds_to_close_now(market_key=market_key, now_utc=now_utc)
            due_rows.append(
                {
                    "market_key": market_key,
                    "token_up": token_up,
                    "token_down": token_down,
                    "condition_id": condition_id,
                    "due_top": due_top,
                    "due_book": due_book,
                    "due_trades": due_trades,
                    "pending": market_key in self.pending_by_market,
                    "seconds_to_close": sec_to_close,
                    "hot": sec_to_close <= (int(self.cfg.entry_cutoff_sec) + 120),
                }
            )
        if not due_rows:
            return []

        max_markets = max(1, int(self.cfg.api_max_markets_per_cycle))
        pending_rows = [row for row in due_rows if bool(row["pending"])]
        selected: list[dict[str, Any]] = list(pending_rows)

        remaining_rows = [row for row in due_rows if not bool(row["pending"])]
        hot_rows = sorted(
            [row for row in remaining_rows if bool(row["hot"])],
            key=lambda row: int(row["seconds_to_close"]),
        )
        cold_rows = sorted(
            [row for row in remaining_rows if not bool(row["hot"])],
            key=lambda row: str(row["market_key"]),
        )
        rot_pool = hot_rows + cold_rows
        if rot_pool:
            cursor = int(self._api_market_scan_cursor) % len(rot_pool)
            rotated_pool = rot_pool[cursor:] + rot_pool[:cursor]
        else:
            cursor = 0
            rotated_pool = []

        remaining_slots = max(0, int(max_markets) - len(selected))
        if remaining_slots > 0 and rotated_pool:
            selected.extend(rotated_pool[:remaining_slots])
        if rot_pool:
            self._api_market_scan_cursor = (cursor + max(1, remaining_slots)) % len(rot_pool)

        out: list[tuple[str, str, str, str, bool, bool, bool]] = []
        for row in selected:
            out.append(
                (
                    str(row["market_key"]),
                    str(row["token_up"]),
                    str(row["token_down"]),
                    str(row["condition_id"]),
                    bool(row["due_top"]),
                    bool(row["due_book"]),
                    bool(row["due_trades"]),
                )
            )
        return out

    @staticmethod
    def _is_retryable_error_message(message: str) -> bool:
        txt = str(message or "").strip().lower()
        if not txt:
            return False
        return (
            "timeout" in txt
            or "timed out" in txt
            or "429" in txt
            or "rate limit" in txt
            or "too many requests" in txt
            or "status_code=5" in txt
            or "status code 5" in txt
            or "http error 5" in txt
        )

    def _mark_endpoint_outcome(self, *, endpoint: str, ok: bool, error_message: str = "") -> None:
        key = str(endpoint or "").strip().lower() or "book"
        if not bool(self.cfg.api_backoff_on_error):
            self._api_endpoint_backoff_ms[key] = 0
            self._api_endpoint_error_streak[key] = 0
            if not ok:
                self._api_last_error_by_endpoint[key] = str(error_message or "")
            return

        base = self._base_interval_ms_for_endpoint(key)
        current_backoff = int(max(0, int(self._api_endpoint_backoff_ms.get(key, 0))))
        if ok:
            self._api_endpoint_error_streak[key] = 0
            self._api_last_error_by_endpoint[key] = ""
            if current_backoff <= 0:
                self._api_endpoint_backoff_ms[key] = 0
                return
            decayed = int(current_backoff * 0.50)
            if decayed < int(max(10, base // 5)):
                decayed = 0
            self._api_endpoint_backoff_ms[key] = int(max(0, decayed))
            return

        streak = int(self._api_endpoint_error_streak.get(key, 0)) + 1
        self._api_endpoint_error_streak[key] = streak
        self._api_last_error_by_endpoint[key] = str(error_message or "")
        retryable = self._is_retryable_error_message(str(error_message or ""))
        initial = int(max(25, base // 2))
        if current_backoff <= 0:
            current_backoff = initial
        growth = 2.0 if retryable else 1.4
        next_backoff = int(current_backoff * growth)
        max_backoff_ms = int(max(base, 8_000))
        self._api_endpoint_backoff_ms[key] = int(max(25, min(next_backoff, max_backoff_ms)))

    def _respect_rps_guard(self) -> None:
        max_rps = max(1.0, float(self.cfg.api_max_rps_guard))
        if max_rps <= EPS:
            return
        now_ms = int(time.time() * 1000)
        self._trim_recent_events(queue=self._api_global_requests_1s, now_ms=now_ms, window_ms=1_000)
        if len(self._api_global_requests_1s) < int(max_rps):
            return
        oldest = int(self._api_global_requests_1s[0]) if self._api_global_requests_1s else now_ms
        sleep_ms = max(1, (oldest + 1_000) - now_ms)
        time.sleep(float(sleep_ms) / 1000.0)

    def _trade_row_key(self, *, row: dict[str, Any]) -> str:
        trade_id = str(row.get("id") or row.get("trade_id") or "").strip()
        ts = str(row.get("timestamp") or row.get("timestamp_utc") or "").strip()
        asset = str(row.get("asset") or "").strip()
        if trade_id:
            return f"{trade_id}:{ts}"
        if ts or asset:
            return f"{asset}:{ts}"
        return ""

    def _dedupe_trades_rows(self, *, market_key: str, rows: list[dict[str, Any]]) -> int:
        key = str(market_key or "").strip()
        if not key:
            return 0
        seen = self._seen_trades_by_market.setdefault(key, set())
        queue = self._seen_trades_queue_by_market.setdefault(key, deque())
        new_rows = 0
        for row in rows:
            row_key = self._trade_row_key(row=row)
            if not row_key:
                continue
            if row_key in seen:
                continue
            seen.add(row_key)
            queue.append(row_key)
            new_rows += 1
        while len(queue) > 4000:
            old = queue.popleft()
            seen.discard(old)
        return int(new_rows)

    def _perf_snapshot(self) -> dict[str, float]:
        now_ms = int(time.time() * 1000)
        latencies: list[float] = []
        req_1m = 0
        err_1m = 0
        for endpoint_key, queue in self._api_events_by_endpoint.items():
            self._trim_recent_events(queue=queue, now_ms=now_ms, window_ms=60_000)
            if str(endpoint_key) == "ws":
                continue
            req_1m += len(queue)
            for _ts_ms, latency_ms, ok in queue:
                latencies.append(float(latency_ms))
                if not bool(ok):
                    err_1m += 1
        p95_api_latency_ms = 0.0
        if latencies:
            latencies_sorted = sorted(latencies)
            idx = min(len(latencies_sorted) - 1, int(0.95 * (len(latencies_sorted) - 1)))
            p95_api_latency_ms = float(latencies_sorted[idx])
        self._trim_recent_events(queue=self._api_skip_unchanged_events, now_ms=now_ms, window_ms=60_000)
        skip_unchanged_rate = (
            (float(len(self._api_skip_unchanged_events)) / float(req_1m))
            if req_1m > 0
            else 0.0
        )
        return {
            "loop_cycle_ms": float(max(0.0, self._last_loop_cycle_ms)),
            "data_freshness_ms": float(max(0.0, self._last_data_freshness_ms)),
            "decision_latency_ms": float(max(0.0, self._last_decision_latency_ms)),
            "p95_api_latency_ms": float(max(0.0, p95_api_latency_ms)),
            "errors_last_1m": float(err_1m),
            "requests_last_1m": float(req_1m),
            "skip_unchanged_rate": float(max(0.0, min(1.0, skip_unchanged_rate))),
        }

    def _slug_from_url(self, url_or_slug: str) -> str:
        raw = str(url_or_slug or "").strip()
        if not raw:
            return ""
        if raw.startswith(("http://", "https://")):
            try:
                parsed = urllib.parse.urlparse(raw)
                parts = [p for p in str(parsed.path or "").split("/") if p]
                if "event" in parts:
                    idx = parts.index("event")
                    if idx + 1 < len(parts):
                        return str(parts[idx + 1]).strip().lower()
            except Exception:
                return ""
            return ""
        return raw.lower()

    def _roll_slug_to_now_bucket(self, *, slug: str, now_utc: datetime) -> str:
        txt = str(slug or "").strip().lower()
        # Example: btc-updown-5m-1774715400 -> btc-updown-5m-<current_bucket_ts>
        m = re.match(r"^([a-z0-9]+-updown-([0-9]+)m-)([0-9]+)$", txt)
        if m is None:
            return txt
        prefix = str(m.group(1))
        try:
            bucket_min = int(m.group(2))
        except Exception:
            bucket_min = max(1, int(self.api_collector.config.market.bucket_min)) if self.api_collector is not None else 5
        bucket_min = max(1, int(bucket_min))
        minute = (int(now_utc.minute) // bucket_min) * bucket_min
        bucket_start = now_utc.replace(minute=minute, second=0, microsecond=0)
        return f"{prefix}{int(bucket_start.timestamp())}"

    def _next_bucket_slug_if_prewarm(self, *, slug: str, now_utc: datetime) -> str:
        if not bool(self.cfg.api_prewarm_next_market):
            return ""
        txt = str(slug or "").strip().lower()
        m = re.match(r"^([a-z0-9]+-updown-([0-9]+)m-)([0-9]+)$", txt)
        if m is None:
            return ""
        prefix = str(m.group(1))
        try:
            bucket_min = int(m.group(2))
        except Exception:
            return ""
        bucket_min = max(1, int(bucket_min))
        minute = (int(now_utc.minute) // bucket_min) * bucket_min
        bucket_start = now_utc.replace(minute=minute, second=0, microsecond=0)
        next_start = bucket_start + timedelta(minutes=bucket_min)
        remaining_sec = max(0.0, (next_start - now_utc).total_seconds())
        if remaining_sec > float(max(0, int(self.cfg.api_prewarm_sec))):
            return ""
        return f"{prefix}{int(next_start.timestamp())}"

    def _manual_api_slugs(self, *, now_utc: datetime) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for item in self.cfg.api_market_slugs:
            base_slug = self._slug_from_url(str(item))
            slug = self._roll_slug_to_now_bucket(slug=base_slug, now_utc=now_utc)
            if slug and slug not in seen:
                seen.add(slug)
                out.append(slug)
            prewarm_slug = self._next_bucket_slug_if_prewarm(slug=slug, now_utc=now_utc)
            if prewarm_slug and prewarm_slug not in seen:
                seen.add(prewarm_slug)
                out.append(prewarm_slug)
        for item in self.cfg.api_market_urls:
            base_slug = self._slug_from_url(str(item))
            slug = self._roll_slug_to_now_bucket(slug=base_slug, now_utc=now_utc)
            if slug and slug not in seen:
                seen.add(slug)
                out.append(slug)
            prewarm_slug = self._next_bucket_slug_if_prewarm(slug=slug, now_utc=now_utc)
            if prewarm_slug and prewarm_slug not in seen:
                seen.add(prewarm_slug)
                out.append(prewarm_slug)
        return out

    def _market_key_prefix_from_slug(self, slug: str) -> str:
        parts = [p for p in str(slug or "").strip().lower().split("-") if p]
        coin = parts[0] if parts else "mkt"
        coin = re.sub(r"[^a-z0-9]+", "", coin)
        if not coin:
            coin = "mkt"
        return f"{coin.upper()}5M"

    def _build_market_key_for_slug(self, *, slug: str, close_utc: str, fallback_market_key: str) -> str:
        if close_utc:
            return f"{self._market_key_prefix_from_slug(slug)}_{close_utc}"
        return str(fallback_market_key or "").strip()

    def _resolve_api_markets_for_now(self, *, now_utc: datetime) -> list[tuple[str, str, str, str]]:
        if self.api_collector is None:
            return []
        slugs = self._manual_api_slugs(now_utc=now_utc)
        if not slugs:
            count = max(1, int(self.cfg.api_markets_count))
            bucket_min = max(1, int(self.api_collector.config.market.bucket_min))
            seen_slugs: set[str] = set()
            for idx in range(count):
                at_utc = now_utc + timedelta(minutes=(idx * bucket_min))
                slug = self.api_collector._build_slug(at_utc)  # noqa: SLF001
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                slugs.append(slug)

        resolved_markets: list[tuple[str, str, str, str]] = []
        ws_pairs: list[tuple[str, str]] = []
        for slug in slugs:
            cached = self._api_market_cache_by_slug.get(slug)
            if cached is not None:
                market_key, token_up, token_down, _condition_id = cached
                resolved_markets.append(cached)
                ws_pairs.append((token_up, token_down))
                continue

            try:
                if slug != self.api_collector.market.slug or not self.api_collector.market.market_key:
                    self.api_collector._resolve_market(slug=slug)  # noqa: SLF001
            except Exception as exc:  # noqa: BLE001
                self._warn_api_throttled(f"market_resolve_failed slug={slug}: {exc}")
                continue

            fallback_market_key = str(self.api_collector.market.market_key or "").strip()
            close_utc = str(self.api_collector.market.market_close_utc or "").strip()
            market_key = self._build_market_key_for_slug(
                slug=slug,
                close_utc=close_utc,
                fallback_market_key=fallback_market_key,
            )
            token_up = str(self.api_collector.market.token_up or "").strip()
            token_down = str(self.api_collector.market.token_down or "").strip()
            condition_id = str(self.api_collector.market.condition_id or "").strip()
            if not market_key or not token_up or not token_down:
                self._warn_api_throttled(f"market_key_missing slug={slug}")
                continue
            resolved = (market_key, token_up, token_down, condition_id)
            self._api_market_cache_by_slug[slug] = resolved
            resolved_markets.append(resolved)
            ws_pairs.append((token_up, token_down))

        if self._api_ws_feed is not None and ws_pairs:
            self._api_ws_feed.set_pairs(ws_pairs)
        return resolved_markets

    def _book_pair_hash(self, *, up_book: dict[str, Any], down_book: dict[str, Any]) -> str:
        up_hash = str(up_book.get("hash", "")).strip()
        down_hash = str(down_book.get("hash", "")).strip()
        if up_hash and down_hash:
            return f"{up_hash}:{down_hash}"
        up_a = up_book.get("asks") if isinstance(up_book.get("asks"), list) else []
        down_a = down_book.get("asks") if isinstance(down_book.get("asks"), list) else []
        up_b = up_book.get("bids") if isinstance(up_book.get("bids"), list) else []
        down_b = down_book.get("bids") if isinstance(down_book.get("bids"), list) else []
        if up_a and down_a and up_b and down_b:
            return (
                f"pxsz:{up_a[0].get('price')}:{up_a[0].get('size')}:"
                f"{down_a[0].get('price')}:{down_a[0].get('size')}:"
                f"{up_b[0].get('price')}:{up_b[0].get('size')}:"
                f"{down_b[0].get('price')}:{down_b[0].get('size')}"
            )
        up_ts = str(up_book.get("timestamp", "")).strip()
        down_ts = str(down_book.get("timestamp", "")).strip()
        if up_ts and down_ts:
            return f"ts:{up_ts}:{down_ts}"
        return ""

    def _timed_fetch_book(self, token_id: str) -> tuple[dict[str, Any], float]:
        assert self.api_collector is not None
        started = time.perf_counter()
        book = self.api_collector._fetch_book(token_id)  # noqa: SLF001
        return book, (time.perf_counter() - started) * 1000.0

    def _timed_fetch_midpoint(self, token_id: str) -> tuple[float | None, float]:
        assert self.api_collector is not None
        started = time.perf_counter()
        mid = self.api_collector._fetch_midpoint(token_id)  # noqa: SLF001
        return mid, (time.perf_counter() - started) * 1000.0

    def _timed_fetch_trades(self, condition_id: str) -> tuple[list[dict[str, Any]], float]:
        assert self.api_collector is not None
        started = time.perf_counter()
        rows = self.api_collector._fetch_trades(condition_id)  # noqa: SLF001
        return rows, (time.perf_counter() - started) * 1000.0

    def _fetch_books_rest(self, *, token_up: str, token_down: str) -> tuple[dict[str, Any], dict[str, Any]]:
        assert self.api_collector is not None
        self._respect_rps_guard()
        started = time.perf_counter()
        if self._api_pool is not None:
            up_future = self._api_pool.submit(self._timed_fetch_book, token_up)
            down_future = self._api_pool.submit(self._timed_fetch_book, token_down)
            timeout_sec = max(0.05, float(self.cfg.api_request_timeout_ms) / 1000.0)
            try:
                up_book, up_latency = up_future.result(timeout=timeout_sec)
                down_book, down_latency = down_future.result(timeout=timeout_sec)
                self._record_api_event(endpoint="book", latency_ms=float(up_latency), ok=True)
                self._record_api_event(endpoint="book", latency_ms=float(down_latency), ok=True)
                self._mark_endpoint_outcome(endpoint="book", ok=True)
                return up_book, down_book
            except Exception as exc:
                up_future.cancel()
                down_future.cancel()
                self._record_api_event(endpoint="book", latency_ms=(time.perf_counter() - started) * 1000.0, ok=False)
                self._mark_endpoint_outcome(endpoint="book", ok=False, error_message=str(exc))
                raise
        try:
            out = (
                self.api_collector._fetch_book(token_up),  # noqa: SLF001
                self.api_collector._fetch_book(token_down),  # noqa: SLF001
            )
            self._record_api_event(endpoint="book", latency_ms=(time.perf_counter() - started) * 1000.0, ok=True)
            self._mark_endpoint_outcome(endpoint="book", ok=True)
            return out
        except Exception as exc:
            self._record_api_event(endpoint="book", latency_ms=(time.perf_counter() - started) * 1000.0, ok=False)
            self._mark_endpoint_outcome(endpoint="book", ok=False, error_message=str(exc))
            raise

    def _fetch_books_rest_bulk(
        self,
        *,
        pairs: list[tuple[str, str]],
    ) -> tuple[dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]], str]:
        assert self.api_collector is not None
        if not pairs:
            return {}, ""
        unique_pairs = list(dict.fromkeys((str(up), str(down)) for up, down in pairs))
        out: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]] = {}
        last_error = ""
        timeout_sec = max(0.05, float(self.cfg.api_request_timeout_ms) / 1000.0)
        # Fast path: batch endpoint (POST /books) for all unique token IDs.
        try:
            self._respect_rps_guard()
            started_batch = time.perf_counter()
            token_ids: list[str] = []
            for token_up, token_down in unique_pairs:
                token_ids.extend([str(token_up), str(token_down)])
            books_by_token = self.api_collector._fetch_books(token_ids)  # noqa: SLF001
            self._record_api_event(endpoint="book", latency_ms=(time.perf_counter() - started_batch) * 1000.0, ok=True)
            self._mark_endpoint_outcome(endpoint="book", ok=True)
            for token_up, token_down in unique_pairs:
                up_book = books_by_token.get(str(token_up))
                down_book = books_by_token.get(str(token_down))
                if up_book is None or down_book is None:
                    continue
                out[(token_up, token_down)] = (up_book, down_book)
            if len(out) == len(unique_pairs):
                return out, ""
        except Exception as exc:
            last_error = str(exc)
            self._record_api_event(endpoint="book", latency_ms=(time.perf_counter() - started_batch) * 1000.0, ok=False)
            self._mark_endpoint_outcome(endpoint="book", ok=False, error_message=last_error)
            # Slow path fallback remains below.
            pass
        if self._api_pool is None:
            for token_up, token_down in unique_pairs:
                if (token_up, token_down) in out:
                    continue
                try:
                    self._respect_rps_guard()
                    started_one = time.perf_counter()
                    up_book = self.api_collector._fetch_book(token_up)  # noqa: SLF001
                    down_book = self.api_collector._fetch_book(token_down)  # noqa: SLF001
                    self._record_api_event(endpoint="book", latency_ms=(time.perf_counter() - started_one) * 1000.0, ok=True)
                    self._mark_endpoint_outcome(endpoint="book", ok=True)
                    out[(token_up, token_down)] = (up_book, down_book)
                except Exception as exc:
                    last_error = str(exc)
                    self._record_api_event(endpoint="book", latency_ms=(time.perf_counter() - started_one) * 1000.0, ok=False)
                    self._mark_endpoint_outcome(endpoint="book", ok=False, error_message=last_error)
            return out, last_error
        futures: dict[Any, str] = {}
        needed_tokens: set[str] = set()
        for token_up, token_down in unique_pairs:
            if (token_up, token_down) in out:
                continue
            needed_tokens.add(str(token_up))
            needed_tokens.add(str(token_down))

        token_books: dict[str, dict[str, Any]] = {}
        workers = max(1, int(self.cfg.api_parallel_workers))
        batches = max(1, (len(needed_tokens) + workers - 1) // workers)
        deadline = time.perf_counter() + (timeout_sec * float(batches))
        for token_id in needed_tokens:
            self._respect_rps_guard()
            future = self._api_pool.submit(self._timed_fetch_book, token_id)
            futures[future] = token_id

        try:
            remaining = max(0.001, deadline - time.perf_counter())
            for future in concurrent.futures.as_completed(list(futures.keys()), timeout=remaining):
                token_id = futures.get(future, "")
                try:
                    book, latency_ms = future.result()
                    token_books[token_id] = book
                    self._record_api_event(endpoint="book", latency_ms=float(latency_ms), ok=True)
                    self._mark_endpoint_outcome(endpoint="book", ok=True)
                except Exception as exc:
                    last_error = str(exc)
                    self._record_api_event(endpoint="book", latency_ms=float(timeout_sec * 1000.0), ok=False)
                    self._mark_endpoint_outcome(endpoint="book", ok=False, error_message=last_error)
        except concurrent.futures.TimeoutError:
            last_error = "books_bulk_timeout"

        for future in list(futures.keys()):
            if not future.done():
                future.cancel()

        for token_up, token_down in unique_pairs:
            if (token_up, token_down) in out:
                continue
            up_book = token_books.get(str(token_up))
            down_book = token_books.get(str(token_down))
            if up_book is None or down_book is None:
                continue
            out[(token_up, token_down)] = (up_book, down_book)
        return out, last_error

    def _fetch_midpoints_bulk(self, *, tokens: list[str]) -> tuple[dict[str, float | None], str]:
        assert self.api_collector is not None
        unique_tokens = list(dict.fromkeys(str(token).strip() for token in tokens if str(token).strip()))
        if not unique_tokens:
            return {}, ""
        out: dict[str, float | None] = {}
        last_error = ""
        timeout_sec = max(0.05, float(self.cfg.api_request_timeout_ms) / 1000.0)

        if self._api_pool is None:
            for token_id in unique_tokens:
                self._respect_rps_guard()
                started = time.perf_counter()
                try:
                    out[token_id] = self.api_collector._fetch_midpoint(token_id)  # noqa: SLF001
                    self._record_api_event(endpoint="top", latency_ms=(time.perf_counter() - started) * 1000.0, ok=True)
                    self._mark_endpoint_outcome(endpoint="top", ok=True)
                except Exception as exc:
                    last_error = str(exc)
                    out[token_id] = None
                    self._record_api_event(endpoint="top", latency_ms=(time.perf_counter() - started) * 1000.0, ok=False)
                    self._mark_endpoint_outcome(endpoint="top", ok=False, error_message=last_error)
            return out, last_error

        futures: dict[Any, str] = {}
        workers = max(1, int(self.cfg.api_parallel_workers))
        batches = max(1, (len(unique_tokens) + workers - 1) // workers)
        deadline = time.perf_counter() + (timeout_sec * float(batches))
        for token_id in unique_tokens:
            self._respect_rps_guard()
            future = self._api_pool.submit(self._timed_fetch_midpoint, token_id)
            futures[future] = token_id

        try:
            remaining = max(0.001, deadline - time.perf_counter())
            for future in concurrent.futures.as_completed(list(futures.keys()), timeout=remaining):
                token_id = futures.get(future, "")
                try:
                    midpoint, latency_ms = future.result()
                    out[token_id] = midpoint
                    self._record_api_event(endpoint="top", latency_ms=float(latency_ms), ok=True)
                    self._mark_endpoint_outcome(endpoint="top", ok=True)
                except Exception as exc:
                    last_error = str(exc)
                    out[token_id] = None
                    self._record_api_event(endpoint="top", latency_ms=float(timeout_sec * 1000.0), ok=False)
                    self._mark_endpoint_outcome(endpoint="top", ok=False, error_message=last_error)
        except concurrent.futures.TimeoutError:
            last_error = "midpoints_bulk_timeout"
        for future in list(futures.keys()):
            if not future.done():
                future.cancel()
        return out, last_error

    def _fetch_trades_for_markets(
        self,
        *,
        markets: list[tuple[str, str]],
        use_pool: bool = True,
    ) -> str:
        assert self.api_collector is not None
        todo = [(str(market_key), str(condition_id)) for market_key, condition_id in markets if str(condition_id).strip()]
        if not todo:
            return ""
        timeout_sec = max(0.05, float(self.cfg.api_request_timeout_ms) / 1000.0)
        last_error = ""

        if (self._api_pool is None) or (not bool(use_pool)):
            for market_key, condition_id in todo:
                self._respect_rps_guard()
                started = time.perf_counter()
                try:
                    rows = self.api_collector._fetch_trades(condition_id)  # noqa: SLF001
                    self._dedupe_trades_rows(market_key=market_key, rows=rows)
                    self._record_api_event(endpoint="trades", latency_ms=(time.perf_counter() - started) * 1000.0, ok=True)
                    self._mark_endpoint_outcome(endpoint="trades", ok=True)
                except Exception as exc:
                    last_error = str(exc)
                    self._record_api_event(endpoint="trades", latency_ms=(time.perf_counter() - started) * 1000.0, ok=False)
                    self._mark_endpoint_outcome(endpoint="trades", ok=False, error_message=last_error)
            return last_error

        futures: dict[Any, str] = {}
        workers = max(1, int(self.cfg.api_parallel_workers))
        batches = max(1, (len(todo) + workers - 1) // workers)
        deadline = time.perf_counter() + (timeout_sec * float(batches))
        for market_key, condition_id in todo:
            self._respect_rps_guard()
            future = self._api_pool.submit(self._timed_fetch_trades, condition_id)
            futures[future] = market_key
        try:
            remaining = max(0.001, deadline - time.perf_counter())
            for future in concurrent.futures.as_completed(list(futures.keys()), timeout=remaining):
                market_key = futures.get(future, "")
                try:
                    rows, latency_ms = future.result()
                    rows_list = rows if isinstance(rows, list) else []
                    self._dedupe_trades_rows(market_key=market_key, rows=[row for row in rows_list if isinstance(row, dict)])
                    self._record_api_event(endpoint="trades", latency_ms=float(latency_ms), ok=True)
                    self._mark_endpoint_outcome(endpoint="trades", ok=True)
                except Exception as exc:
                    last_error = str(exc)
                    self._record_api_event(endpoint="trades", latency_ms=float(timeout_sec * 1000.0), ok=False)
                    self._mark_endpoint_outcome(endpoint="trades", ok=False, error_message=last_error)
        except concurrent.futures.TimeoutError:
            last_error = "trades_bulk_timeout"
        for future in list(futures.keys()):
            if not future.done():
                future.cancel()
        return last_error

    def _poll_trades_future(self) -> None:
        future = self._api_trades_future
        if future is None:
            return
        if not future.done():
            return
        self._api_trades_future = None
        try:
            trades_error = str(future.result() or "").strip()
        except Exception as exc:  # noqa: BLE001
            self._warn_api_throttled(f"trades_fetch_error:{exc}")
            return
        if trades_error:
            self._warn_api_throttled(f"trades_fetch_error:{trades_error}")

    def _start_trades_fetch(self, *, trades_due: list[tuple[str, str]]) -> None:
        if not trades_due:
            return
        if self._api_pool is None:
            trades_error = self._fetch_trades_for_markets(markets=trades_due, use_pool=False)
            if trades_error:
                self._warn_api_throttled(f"trades_fetch_error:{trades_error}")
            return
        self._poll_trades_future()
        if self._api_trades_future is not None and not self._api_trades_future.done():
            return
        due_copy = [(str(market_key), str(condition_id)) for market_key, condition_id in trades_due]
        self._api_trades_future = self._api_pool.submit(
            self._fetch_trades_for_markets,
            markets=due_copy,
            use_pool=False,
        )

    def _fetch_books_ws(
        self,
        *,
        token_up: str,
        token_down: str,
        last_pair_hash: str = "",
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, float | None, bool, str, str, bool]:
        if self._api_ws_feed is None:
            return None, None, None, False, "ws_disabled", "", False
        return self._api_ws_feed.get_pair_books(
            token_up=token_up,
            token_down=token_down,
            depth=max(1, int(self.cfg.api_book_depth)),
            last_pair_hash=str(last_pair_hash or "").strip(),
            skip_unchanged=bool(self.cfg.api_skip_unchanged_book),
        )

    def _snapshot_from_books(
        self,
        *,
        up_book: dict[str, Any],
        down_book: dict[str, Any],
        market_key: str,
        token_up: str,
        token_down: str,
        source: str,
    ) -> dict[str, Any] | None:
        depth = max(1, int(self.cfg.api_book_depth))
        up_bids = normalize_levels(up_book.get("bids"), depth=depth, reverse=True)
        up_asks = normalize_levels(up_book.get("asks"), depth=depth, reverse=False)
        down_bids = normalize_levels(down_book.get("bids"), depth=depth, reverse=True)
        down_asks = normalize_levels(down_book.get("asks"), depth=depth, reverse=False)

        if not up_asks or not down_asks:
            self._warn_api_throttled("book_missing_asks")
            return None
        if not up_bids or not down_bids:
            self._warn_api_throttled("book_missing_bids")
            return None

        yes_ask_top = _safe_float(up_asks[0].get("price"), default=None)
        yes_bid_top = _safe_float(up_bids[0].get("price"), default=None)
        no_ask_top = _safe_float(down_asks[0].get("price"), default=None)
        no_bid_top = _safe_float(down_bids[0].get("price"), default=None)
        if yes_ask_top is None or no_ask_top is None:
            self._warn_api_throttled("book_invalid_prices")
            return None
        yes_ask_size = self._depth_size_sum(up_asks)
        no_ask_size = self._depth_size_sum(down_asks)
        yes_min_order_size = _safe_float(up_book.get("min_order_size"), default=None)
        no_min_order_size = _safe_float(down_book.get("min_order_size"), default=None)

        yes_ask = float(yes_ask_top)
        no_ask = float(no_ask_top)
        yes_bid = None if yes_bid_top is None else float(yes_bid_top)
        no_bid = None if no_bid_top is None else float(no_bid_top)
        if bool(self.cfg.api_use_effective_quotes):
            qty_yes = float(self._size_for_leg(float(yes_ask_top)))
            qty_no = float(self._size_for_leg(float(no_ask_top)))
            yes_ask_eff = self._vwap_for_target_size(up_asks, qty_yes)
            no_ask_eff = self._vwap_for_target_size(down_asks, qty_no)
            if yes_ask_eff is None or no_ask_eff is None:
                self._warn_api_throttled("book_insufficient_depth_for_target_size")
                return None
            yes_bid_eff = self._vwap_for_target_size(up_bids, qty_yes)
            no_bid_eff = self._vwap_for_target_size(down_bids, qty_no)
            yes_ask = float(yes_ask_eff)
            no_ask = float(no_ask_eff)
            if yes_bid_eff is not None:
                yes_bid = float(yes_bid_eff)
            if no_bid_eff is not None:
                no_bid = float(no_bid_eff)

        ts_utc = _iso_utc_now()
        now_ms = int(time.time() * 1000)
        up_ts_ms = int(_safe_float(up_book.get("timestamp"), default=float(now_ms)) or now_ms)
        down_ts_ms = int(_safe_float(down_book.get("timestamp"), default=float(now_ms)) or now_ms)
        data_age_ms = max(0.0, float(now_ms - min(up_ts_ms, down_ts_ms)))
        return {
            "ts_utc": ts_utc,
            "market_key": market_key,
            "token_up": token_up,
            "token_down": token_down,
            "feed_source": str(source or "").strip(),
            "data_age_ms": float(data_age_ms),
            "yes_ask": float(yes_ask),
            "yes_bid": None if yes_bid is None else float(yes_bid),
            "yes_ask_size": float(yes_ask_size),
            "yes_spread": None if yes_bid is None else float(yes_ask) - float(yes_bid),
            "no_ask": float(no_ask),
            "no_bid": None if no_bid is None else float(no_bid),
            "no_ask_size": float(no_ask_size),
            "no_spread": None if no_bid is None else float(no_ask) - float(no_bid),
            "sum_ask": float(yes_ask) + float(no_ask),
            "yes_min_order_size": yes_min_order_size,
            "no_min_order_size": no_min_order_size,
            "seconds_to_close": self._seconds_to_close(ts_utc=ts_utc, market_key=market_key),
        }

    def _read_api_snapshots(self) -> list[dict[str, Any]]:
        if self.api_collector is None:
            return []
        now_utc = datetime.now(timezone.utc)
        now_ms = int(now_utc.timestamp() * 1000)
        try:
            resolved_markets = self._resolve_api_markets_for_now(now_utc=now_utc)
            if not resolved_markets:
                self._warn_api_throttled("market_key_missing")
                return []
            selected_markets = self._select_markets_for_cycle(
                resolved_markets=resolved_markets,
                now_utc=now_utc,
                now_ms=now_ms,
            )
            if not selected_markets:
                return []

            snapshots: list[dict[str, Any]] = []
            rest_fallback_candidates: dict[str, dict[str, Any]] = {}
            top_probe_candidates: dict[str, dict[str, Any]] = {}
            trades_due: list[tuple[str, str]] = []

            for market_key, token_up, token_down, condition_id, due_top, due_book, due_trades in selected_markets:
                if due_trades:
                    trades_due.append((market_key, condition_id))
                if not (due_top or due_book):
                    continue

                last_hash = self._last_api_book_pair_hash_by_market.get(market_key, "")
                need_rest = True
                market_has_pending = market_key in self.pending_by_market

                if self._api_ws_feed is not None:
                    started_ws = time.perf_counter()
                    up_book_ws, down_book_ws, ws_age_sec, ws_connected, ws_error, pair_hash_ws, ws_unchanged = self._fetch_books_ws(
                        token_up=token_up,
                        token_down=token_down,
                        last_pair_hash=last_hash,
                    )
                    ws_ok = bool(up_book_ws is not None and down_book_ws is not None and ws_age_sec is not None)
                    self._record_api_event(
                        endpoint="ws",
                        latency_ms=(time.perf_counter() - started_ws) * 1000.0,
                        ok=ws_ok,
                    )
                    if bool(ws_unchanged):
                        self._record_skip_unchanged()
                        if due_top:
                            self._schedule_market_endpoint(
                                market_key=market_key,
                                endpoint="top",
                                now_ms=now_ms,
                            )
                        if due_book:
                            self._schedule_market_endpoint(
                                market_key=market_key,
                                endpoint="book",
                                now_ms=now_ms,
                            )
                        need_rest = False
                        continue
                    if up_book_ws is not None and down_book_ws is not None and ws_age_sec is not None:
                        stale_limit = float(self.cfg.api_ws_stale_sec)
                        if ws_age_sec <= stale_limit + EPS:
                            if not pair_hash_ws:
                                pair_hash_ws = self._book_pair_hash(up_book=up_book_ws, down_book=down_book_ws)
                            if (
                                bool(self.cfg.api_skip_unchanged_book)
                                and pair_hash_ws
                                and pair_hash_ws == last_hash
                            ):
                                self._record_skip_unchanged()
                                if due_top:
                                    self._schedule_market_endpoint(
                                        market_key=market_key,
                                        endpoint="top",
                                        now_ms=now_ms,
                                    )
                                if due_book:
                                    self._schedule_market_endpoint(
                                        market_key=market_key,
                                        endpoint="book",
                                        now_ms=now_ms,
                                    )
                                need_rest = False
                                continue
                            snapshot_ws = self._snapshot_from_books(
                                up_book=up_book_ws,
                                down_book=down_book_ws,
                                market_key=market_key,
                                token_up=token_up,
                                token_down=token_down,
                                source="ws",
                            )
                            if snapshot_ws is not None:
                                snapshots.append(snapshot_ws)
                                if pair_hash_ws:
                                    self._last_api_book_pair_hash_by_market[market_key] = pair_hash_ws
                                if due_top:
                                    self._schedule_market_endpoint(
                                        market_key=market_key,
                                        endpoint="top",
                                        now_ms=now_ms,
                                    )
                                # WS snapshot already has deep levels, so we can also move book schedule.
                                self._schedule_market_endpoint(
                                    market_key=market_key,
                                    endpoint="book",
                                    now_ms=now_ms,
                                )
                                need_rest = False
                                continue
                        else:
                            if not market_has_pending:
                                snapshot_stale = self._snapshot_from_books(
                                    up_book=up_book_ws,
                                    down_book=down_book_ws,
                                    market_key=market_key,
                                    token_up=token_up,
                                    token_down=token_down,
                                    source="ws_stale",
                                )
                                if snapshot_stale is not None:
                                    snapshots.append(snapshot_stale)
                                    if pair_hash_ws:
                                        self._last_api_book_pair_hash_by_market[market_key] = pair_hash_ws
                                self._schedule_market_endpoint(
                                    market_key=market_key,
                                    endpoint="top",
                                    now_ms=now_ms,
                                )
                                self._schedule_market_endpoint(
                                    market_key=market_key,
                                    endpoint="book",
                                    now_ms=now_ms,
                                )
                                need_rest = False
                                continue
                            self._warn_api_throttled(
                                f"ws_stale_fallback age_sec={ws_age_sec:.3f} stale_sec={float(self.cfg.api_ws_stale_sec):.3f}"
                            )
                    else:
                        if ws_connected:
                            self._warn_api_throttled("ws_waiting_initial_book")
                        elif ws_error:
                            self._warn_api_throttled(f"ws_not_connected:{ws_error}")
                        else:
                            self._warn_api_throttled("ws_not_connected")
                    if not bool(self.cfg.api_ws_fallback_rest):
                        if due_top:
                            self._schedule_market_endpoint(
                                market_key=market_key,
                                endpoint="top",
                                now_ms=now_ms,
                            )
                        if due_book:
                            self._schedule_market_endpoint(
                                market_key=market_key,
                                endpoint="book",
                                now_ms=now_ms,
                            )
                        need_rest = False
                        continue

                if not need_rest:
                    continue

                if due_book:
                    rest_fallback_candidates[market_key] = {
                        "market_key": market_key,
                        "token_up": token_up,
                        "token_down": token_down,
                        "last_hash": last_hash,
                        "due_top": bool(due_top),
                        "force_book": True,
                    }
                elif due_top:
                    if self._api_ws_feed is not None:
                        self._schedule_market_endpoint(
                            market_key=market_key,
                            endpoint="top",
                            now_ms=now_ms,
                        )
                        continue
                    top_probe_candidates[market_key] = {
                        "market_key": market_key,
                        "token_up": token_up,
                        "token_down": token_down,
                        "last_hash": last_hash,
                    }

            if top_probe_candidates:
                midpoint_tokens: list[str] = []
                for row in top_probe_candidates.values():
                    midpoint_tokens.extend([str(row["token_up"]), str(row["token_down"])])
                midpoints, top_error = self._fetch_midpoints_bulk(tokens=midpoint_tokens)
                if top_error:
                    self._warn_api_throttled(f"top_midpoint_error:{top_error}")
                for market_key, row in top_probe_candidates.items():
                    token_up = str(row["token_up"])
                    token_down = str(row["token_down"])
                    up_mid = _safe_float(midpoints.get(token_up), default=None)
                    down_mid = _safe_float(midpoints.get(token_down), default=None)
                    prev_mid = self._api_midpoint_by_market.get(market_key)
                    changed = False
                    if up_mid is not None and down_mid is not None:
                        if prev_mid is None:
                            changed = True
                        else:
                            prev_up, prev_down = prev_mid
                            if prev_up is None or prev_down is None:
                                changed = True
                            else:
                                changed = (
                                    abs(float(up_mid) - float(prev_up)) > 1e-6
                                    or abs(float(down_mid) - float(prev_down)) > 1e-6
                                )
                        self._api_midpoint_by_market[market_key] = (float(up_mid), float(down_mid))
                    if changed:
                        existing = rest_fallback_candidates.get(market_key)
                        if existing is None:
                            rest_fallback_candidates[market_key] = {
                                "market_key": market_key,
                                "token_up": token_up,
                                "token_down": token_down,
                                "last_hash": str(row.get("last_hash", "")),
                                "due_top": True,
                                "force_book": True,
                            }
                        else:
                            existing["due_top"] = True
                            existing["force_book"] = True
                    else:
                        self._record_skip_unchanged()
                    self._schedule_market_endpoint(
                        market_key=market_key,
                        endpoint="top",
                        now_ms=now_ms,
                    )

            if rest_fallback_candidates:
                pair_books, rest_error = self._fetch_books_rest_bulk(
                    pairs=[
                        (str(row["token_up"]), str(row["token_down"]))
                        for row in rest_fallback_candidates.values()
                    ]
                )
                if rest_error:
                    self._warn_api_throttled(f"books_fetch_error:{rest_error}")
                for market_key, row in rest_fallback_candidates.items():
                    token_up = str(row["token_up"])
                    token_down = str(row["token_down"])
                    last_hash = str(row.get("last_hash", ""))
                    due_top = bool(row.get("due_top", False))
                    books = pair_books.get((token_up, token_down))
                    self._schedule_market_endpoint(
                        market_key=market_key,
                        endpoint="book",
                        now_ms=now_ms,
                    )
                    if due_top:
                        self._schedule_market_endpoint(
                            market_key=market_key,
                            endpoint="top",
                            now_ms=now_ms,
                        )
                    if books is None:
                        continue
                    up_book, down_book = books
                    if self._api_ws_feed is not None:
                        # Prime WS cache with fresh REST snapshot so next cycle can use low-latency WS path
                        # even if initial WS book for this asset pair is delayed right after market rollover.
                        self._api_ws_feed.seed_pair_from_rest(
                            token_up=token_up,
                            token_down=token_down,
                            up_book=up_book,
                            down_book=down_book,
                        )
                    pair_hash = self._book_pair_hash(up_book=up_book, down_book=down_book)
                    if bool(self.cfg.api_skip_unchanged_book) and pair_hash and pair_hash == last_hash:
                        self._record_skip_unchanged()
                        continue
                    snapshot = self._snapshot_from_books(
                        up_book=up_book,
                        down_book=down_book,
                        market_key=market_key,
                        token_up=token_up,
                        token_down=token_down,
                        source=("rest_fallback" if self._api_ws_feed is not None else "rest"),
                    )
                    if snapshot is None:
                        continue
                    snapshots.append(snapshot)
                    if pair_hash:
                        self._last_api_book_pair_hash_by_market[market_key] = pair_hash

            self._poll_trades_future()
            if trades_due:
                self._start_trades_fetch(trades_due=trades_due)
                for market_key, _condition_id in trades_due:
                    self._schedule_market_endpoint(
                        market_key=market_key,
                        endpoint="trades",
                        now_ms=now_ms,
                    )

            return snapshots
        except Exception as exc:  # noqa: BLE001
            self._warn_api_throttled(f"snapshot_error:{exc}")
            return []

    def _warmup_api_state(self) -> None:
        if self.market_source != "api" or self.api_collector is None:
            return
        started = time.perf_counter()
        seeded_books = 0
        try:
            self._suppress_api_metrics = True
            now_utc = datetime.now(timezone.utc)
            now_ms = int(now_utc.timestamp() * 1000)
            resolved_markets = self._resolve_api_markets_for_now(now_utc=now_utc)
            if not resolved_markets:
                return

            for market_key, _token_up, _token_down, _condition_id in resolved_markets:
                self._market_schedule(market_key=market_key, now_ms=now_ms)

            if self._api_ws_feed is not None and bool(self.cfg.api_ws_fallback_rest):
                warmup_count = max(1, min(len(resolved_markets), int(self.cfg.api_max_markets_per_cycle)))
                selected = resolved_markets[:warmup_count]
                pair_to_market: dict[tuple[str, str], str] = {}
                pairs: list[tuple[str, str]] = []
                for market_key, token_up, token_down, _condition_id in selected:
                    key = (str(token_up), str(token_down))
                    pair_to_market[key] = str(market_key)
                    pairs.append(key)
                pair_books, err = self._fetch_books_rest_bulk(pairs=pairs)
                if err:
                    self._warn_api_throttled(f"api_warmup_books_error:{err}")
                for pair_key, books in pair_books.items():
                    token_up, token_down = pair_key
                    up_book, down_book = books
                    self._api_ws_feed.seed_pair_from_rest(
                        token_up=token_up,
                        token_down=token_down,
                        up_book=up_book,
                        down_book=down_book,
                    )
                    market_key = pair_to_market.get((str(token_up), str(token_down)), "")
                    if market_key:
                        pair_hash = self._book_pair_hash(up_book=up_book, down_book=down_book)
                        if pair_hash:
                            self._last_api_book_pair_hash_by_market[str(market_key)] = str(pair_hash)
                    seeded_books += 1

            elapsed_ms = (time.perf_counter() - started) * 1000.0
            print(
                f"[live-mvp] api_warmup resolved_markets={len(resolved_markets)} "
                f"seeded_books={seeded_books} elapsed_ms={elapsed_ms:.1f}"
            )
        except Exception as exc:  # noqa: BLE001
            self._warn_api_throttled(f"api_warmup_error:{exc}")
        finally:
            self._suppress_api_metrics = False

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

    def _depth_size_sum(self, levels: list[dict[str, float]]) -> float:
        total = 0.0
        for level in levels:
            size = _safe_float(level.get("size"), default=0.0) or 0.0
            if size > 0:
                total += float(size)
        return float(total)

    def _vwap_for_target_size(self, levels: list[dict[str, float]], target_size: float) -> float | None:
        need = max(EPS, float(target_size))
        acc_size = 0.0
        acc_notional = 0.0
        for level in levels:
            px = _safe_float(level.get("price"), default=None)
            sz = _safe_float(level.get("size"), default=0.0)
            if px is None or sz is None or sz <= 0:
                continue
            take = min(float(sz), need - acc_size)
            if take <= 0:
                break
            acc_size += float(take)
            acc_notional += float(take) * float(px)
            if acc_size + EPS >= need:
                break
        if acc_size + EPS < need:
            return None
        return float(acc_notional / acc_size)

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
            "feed_source": "file",
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

    def _within_depth(self, size: float | None, *, required_qty: float | None = None) -> bool:
        if size is None:
            return False
        base_qty = float(required_qty) if required_qty is not None else float(self.cfg.stake_usd_per_leg)
        needed = float(base_qty) * float(self.cfg.min_depth_buffer_mult)
        return float(size) + EPS >= needed

    def _size_for_leg(self, price: float) -> float:
        safe_price = max(EPS, float(price))
        stake_qty = float(self.cfg.stake_usd_per_leg) / safe_price
        return max(float(self.cfg.maker_min_size_shares), float(stake_qty))

    def _leg2_size_with_min_notional(self, *, leg2_price: float, leg1_filled_qty: float) -> float:
        _ = float(leg2_price)  # leg2 size follows leg1 position size for hedge consistency.
        base_qty = max(EPS, float(leg1_filled_qty))
        return max(base_qty, float(self.cfg.maker_min_size_shares))

    def _target_profit_pct_for_regime(self, *, regime: str) -> float:
        min_target = float(self.cfg.target_profit_pct)
        max_target = float(self.cfg.target_profit_max_pct)
        if max_target <= min_target + EPS:
            return float(min_target)
        if regime == "HIGH_EDGE":
            return float(max_target)
        if regime == "MID_EDGE":
            return float(min_target + ((max_target - min_target) / 2.0))
        return float(min_target)

    def _leg2_target_max_ask(self, *, entry_ask: float, target_profit_pct: float) -> float:
        # Custos explicitos seguem fora; caller pode embutir buffer (ex.: slippage) em target_profit_pct.
        target_frac = float(target_profit_pct) / 100.0
        return 1.0 - float(entry_ask) - target_frac

    def _classify_edge_regime(self, *, sum_ask: float) -> str:
        if float(sum_ask) <= float(self.cfg.edge_high_max_sum_ask) + EPS:
            return "HIGH_EDGE"
        if float(sum_ask) <= float(self.cfg.edge_mid_max_sum_ask) + EPS:
            return "MID_EDGE"
        if float(sum_ask) <= float(self.cfg.edge_weak_max_sum_ask) + EPS:
            return "LOW_EDGE"
        return "NO_TRADE"

    def _regime_params(self, *, regime: str) -> tuple[str, int]:
        if regime == "HIGH_EDGE":
            return "passive_limit", int(self.cfg.leg2_timeout_high_ms)
        if regime == "MID_EDGE":
            return "passive_limit", int(self.cfg.leg2_timeout_mid_ms)
        if regime == "LOW_EDGE":
            return "passive_limit", int(self.cfg.leg2_timeout_weak_ms)
        return "no_trade", int(self.cfg.leg2_timeout_ms)

    def _update_book_stability(self, *, snapshot: dict[str, Any]) -> int:
        market_key = str(snapshot["market_key"])
        sig = (
            round(float(snapshot["yes_ask"]), 4),
            round(float(snapshot["no_ask"]), 4),
            round(float(_safe_float(snapshot.get("yes_bid"), default=0.0) or 0.0), 4),
            round(float(_safe_float(snapshot.get("no_bid"), default=0.0) or 0.0), 4),
        )
        prev = self._book_signature_by_market.get(market_key)
        if prev == sig:
            self._book_stable_ticks_by_market[market_key] += 1
        else:
            self._book_stable_ticks_by_market[market_key] = 1
            self._book_signature_by_market[market_key] = sig
        return int(self._book_stable_ticks_by_market.get(market_key, 0))

    def _order_filled_from_status(self, payload: dict[str, Any]) -> tuple[bool, float]:
        status = str(payload.get("status") or "").strip().lower()
        filled = _safe_float(payload.get("size_matched") or payload.get("filled_size"), default=0.0) or 0.0
        filled_ok = (float(filled) > EPS) or ("filled" in status) or ("matched" in status)
        avg_price = _safe_float(payload.get("avg_price") or payload.get("price"), default=0.0) or 0.0
        return bool(filled_ok), float(avg_price)

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

    def _mark_unwind_pending(
        self,
        *,
        snapshot: dict[str, Any],
        pending: PendingTrade,
        reason_code: str,
        note: str,
    ) -> None:
        now_ms = self._ts_to_epoch_ms(snapshot["ts_utc"])
        pending.leg2_mode = "force_unwind"
        pending.deadline_ms = min(int(pending.deadline_ms), int(now_ms))
        self._append_action(
            ts_utc=str(snapshot["ts_utc"]),
            market_key=pending.market_key,
            action="retry_unwind_leg1",
            status="retrying",
            reason_code=reason_code,
            leg1_side=pending.leg1_side,
            yes_ask=_safe_float(snapshot.get("yes_ask"), default=None),
            no_ask=_safe_float(snapshot.get("no_ask"), default=None),
            sum_ask=_safe_float(snapshot.get("sum_ask"), default=None),
            seconds_to_close=int(_safe_float(snapshot.get("seconds_to_close"), default=0.0) or 0),
            leg1_price=pending.leg1_ask,
            order_id_leg1=pending.order_id_leg1,
            order_id_leg2=pending.order_id_leg2,
            fill_status_leg1=pending.fill_status_leg1 or "submitted",
            fill_status_leg2=pending.fill_status_leg2,
            unwind_triggered=True,
            note=note,
        )

    def _calc_unwind_loss_bps(self, *, entry_ask: float, unwind_bid: float | None) -> float:
        if unwind_bid is None or float(entry_ask) <= EPS:
            return float("inf")
        loss = max(0.0, float(entry_ask) - float(unwind_bid))
        return (loss / float(entry_ask)) * 10000.0

    def _phase_for_pending(self, *, now_ms: int, pending: PendingTrade) -> tuple[str, int]:
        if int(pending.phase_high_deadline_ms) > int(now_ms):
            return "HIGH_EDGE", int(pending.phase_high_deadline_ms)
        if int(pending.phase_mid_deadline_ms) > int(now_ms):
            return "MID_EDGE", int(pending.phase_mid_deadline_ms)
        if int(pending.phase_weak_deadline_ms) > int(now_ms):
            return "LOW_EDGE", int(pending.phase_weak_deadline_ms)
        return "EXPIRED", int(pending.phase_weak_deadline_ms or pending.deadline_ms)

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
        leg2_target = float(pending.leg2_target_max_ask)
        target_hit = float(leg2_ask) <= (leg2_target + EPS)
        phase_regime, phase_deadline_ms = self._phase_for_pending(now_ms=now_ms, pending=pending)
        if pending.leg2_mode == "force_unwind" or phase_regime == "EXPIRED":
            effective_leg2_mode = "force_unwind"
        else:
            effective_leg2_mode, _ = self._regime_params(regime=phase_regime)
            pending.leg2_mode = str(effective_leg2_mode)
        phase_remaining_ms = max(0, int(phase_deadline_ms - now_ms))

        if self._live_enabled():
            try:
                if effective_leg2_mode == "passive_limit":
                    if pending.order_id_leg2:
                        status_payload = self.executor.get_order_status(pending.order_id_leg2)  # type: ignore[union-attr]
                        filled_ok, avg_price = self._order_filled_from_status(status_payload)
                        pending.fill_status_leg2 = str(status_payload.get("status") or pending.fill_status_leg2 or "unknown")
                        if filled_ok:
                            hedge_price = float(avg_price if avg_price > 0 else leg2_ask)
                            pnl_delta = (1.0 - float(pending.leg1_ask) - float(hedge_price)) * float(
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
                                leg2_price=hedge_price,
                                order_id_leg1=pending.order_id_leg1,
                                order_id_leg2=pending.order_id_leg2,
                                fill_status_leg1=pending.fill_status_leg1 or "submitted",
                                fill_status_leg2=pending.fill_status_leg2,
                                hedge_latency_ms=0,
                                unwind_triggered=False,
                                pnl_delta=pnl_delta,
                                note=f"passive_hedge_filled target_b<={leg2_target:.6f}",
                            )
                            return
                        if now_ms >= int(pending.deadline_ms):
                            cancel_result = self.executor.cancel_open_orders([pending.order_id_leg2])  # type: ignore[union-attr]
                            self._append_action(
                                ts_utc=snapshot["ts_utc"],
                                market_key=market_key,
                                action="retry_unwind_leg1",
                                status="retrying",
                                reason_code="hedge_passive_cancelled_timeout",
                                leg1_side=pending.leg1_side,
                                yes_ask=yes_ask,
                                no_ask=no_ask,
                                sum_ask=sum_ask,
                                seconds_to_close=snapshot["seconds_to_close"],
                                leg1_price=pending.leg1_ask,
                                order_id_leg1=pending.order_id_leg1,
                                order_id_leg2=pending.order_id_leg2,
                                fill_status_leg1=pending.fill_status_leg1,
                                fill_status_leg2=pending.fill_status_leg2,
                                unwind_triggered=True,
                                note=f"passive_leg2_cancelled_after_timeout cancel_result={cancel_result}",
                            )
                            pending.order_id_leg2 = ""
                            pending.fill_status_leg2 = "passive_cancelled_timeout"
                        else:
                            if now_ms >= int(pending.next_hedge_retry_ms):
                                pending.next_hedge_retry_ms = int(
                                    now_ms + max(1500, int(float(self.cfg.poll_interval_sec) * 1000))
                                )
                                self._append_action(
                                    ts_utc=snapshot["ts_utc"],
                                    market_key=market_key,
                                    action="try_hedge",
                                    status="open",
                                    reason_code="hedge_passive_waiting",
                                    leg1_side=pending.leg1_side,
                                    yes_ask=yes_ask,
                                    no_ask=no_ask,
                                    sum_ask=sum_ask,
                                    seconds_to_close=snapshot["seconds_to_close"],
                                    leg1_price=pending.leg1_ask,
                                    leg2_price=leg2_target,
                                    order_id_leg1=pending.order_id_leg1,
                                    order_id_leg2=pending.order_id_leg2,
                                    fill_status_leg1=pending.fill_status_leg1 or "submitted",
                                    fill_status_leg2=pending.fill_status_leg2,
                                    unwind_triggered=False,
                                    note=(
                                        f"phase={phase_regime} passive_order_open "
                                        f"deadline_ms={int(pending.deadline_ms)}"
                                    ),
                                )
                            return
                    elif now_ms < int(pending.deadline_ms):
                        if now_ms < int(pending.next_hedge_retry_ms):
                            return
                        if not pending.leg2_token_id:
                            raise ValueError("missing_leg2_token_id")
                        hedge_size = float(
                            self._leg2_size_with_min_notional(
                                leg2_price=float(leg2_target),
                                leg1_filled_qty=float(pending.quantity),
                            )
                        )
                        if hedge_size <= EPS:
                            raise ValueError("invalid_leg2_size")
                        self._append_action(
                            ts_utc=snapshot["ts_utc"],
                            market_key=market_key,
                            action="try_hedge",
                            status="open",
                            reason_code="hedge_passive_submit",
                            leg1_side=pending.leg1_side,
                            yes_ask=yes_ask,
                            no_ask=no_ask,
                            sum_ask=sum_ask,
                            seconds_to_close=snapshot["seconds_to_close"],
                            leg1_price=pending.leg1_ask,
                            leg2_price=leg2_target,
                            order_id_leg1=pending.order_id_leg1,
                            order_id_leg2=pending.order_id_leg2,
                            fill_status_leg1=pending.fill_status_leg1 or "submitted",
                            fill_status_leg2=pending.fill_status_leg2,
                            unwind_triggered=False,
                            note=(
                                f"phase={phase_regime} passive_submit size={float(hedge_size):.6f} "
                                f"target_b<={float(leg2_target):.6f}"
                            ),
                        )
                        hedge_result = self.executor.place_leg2_hedge(  # type: ignore[union-attr]
                            token_id=pending.leg2_token_id,
                            price=float(leg2_target),
                            size=float(hedge_size),
                            timeout_ms=0,
                            order_type="GTC",
                            post_only=False,
                            require_fill=False,
                        )
                        pending.order_id_leg2 = hedge_result.order_id
                        pending.fill_status_leg2 = hedge_result.status or "submitted"
                        if hedge_result.success:
                            pending.next_hedge_retry_ms = int(
                                now_ms + max(1500, int(float(self.cfg.poll_interval_sec) * 1000))
                            )
                            self._append_action(
                                ts_utc=snapshot["ts_utc"],
                                market_key=market_key,
                                action="try_hedge",
                                status="open",
                                reason_code="hedge_passive_posted",
                                leg1_side=pending.leg1_side,
                                yes_ask=yes_ask,
                                no_ask=no_ask,
                                sum_ask=sum_ask,
                                seconds_to_close=snapshot["seconds_to_close"],
                                leg1_price=pending.leg1_ask,
                                leg2_price=leg2_target,
                                order_id_leg1=pending.order_id_leg1,
                                order_id_leg2=pending.order_id_leg2,
                                fill_status_leg1=pending.fill_status_leg1 or "submitted",
                                fill_status_leg2=pending.fill_status_leg2,
                                unwind_triggered=False,
                                note=f"passive_limit_order_posted timeout_ms={int(pending.deadline_ms - pending.opened_at_ms)}",
                            )
                            return
                        pending.fill_status_leg2 = hedge_result.status or "hedge_post_failed"
                        min_size_shares = _extract_min_size_shares(hedge_result.error or hedge_result.status or "")
                        if min_size_shares is not None:
                            pending.deadline_ms = min(int(pending.deadline_ms), int(now_ms))
                            self._append_action(
                                ts_utc=snapshot["ts_utc"],
                                market_key=market_key,
                                action="try_hedge",
                                status="blocked",
                                reason_code="hedge_passive_min_size",
                                leg1_side=pending.leg1_side,
                                yes_ask=yes_ask,
                                no_ask=no_ask,
                                sum_ask=sum_ask,
                                seconds_to_close=snapshot["seconds_to_close"],
                                leg1_price=pending.leg1_ask,
                                leg2_price=leg2_target,
                                order_id_leg1=pending.order_id_leg1,
                                order_id_leg2=pending.order_id_leg2,
                                fill_status_leg1=pending.fill_status_leg1 or "submitted",
                                fill_status_leg2=pending.fill_status_leg2,
                                hedge_latency_ms=hedge_result.latency_ms,
                                unwind_triggered=False,
                                note=(
                                    f"phase={phase_regime} passive_size={float(hedge_size):.6f} "
                                    f"< maker_min_size={float(min_size_shares):.6f}; force_unwind"
                                ),
                            )
                            # Continue in this cycle to immediate unwind path.
                        if now_ms < int(pending.deadline_ms):
                            retry_wait_ms = max(700, int(float(self.cfg.poll_interval_sec) * 1000))
                            pending.next_hedge_retry_ms = int(now_ms + retry_wait_ms)
                            self._append_action(
                                ts_utc=snapshot["ts_utc"],
                                market_key=market_key,
                                action="try_hedge",
                                status="retrying",
                                reason_code="hedge_passive_post_failed",
                                leg1_side=pending.leg1_side,
                                yes_ask=yes_ask,
                                no_ask=no_ask,
                                sum_ask=sum_ask,
                                seconds_to_close=snapshot["seconds_to_close"],
                                leg1_price=pending.leg1_ask,
                                leg2_price=leg2_target,
                                order_id_leg1=pending.order_id_leg1,
                                order_id_leg2=pending.order_id_leg2,
                                fill_status_leg1=pending.fill_status_leg1 or "submitted",
                                fill_status_leg2=pending.fill_status_leg2,
                                hedge_latency_ms=hedge_result.latency_ms,
                                unwind_triggered=False,
                                note=(
                                    f"phase={phase_regime} passive_post_error="
                                    f"{hedge_result.error or hedge_result.status}; "
                                    f"retry_at_ms={pending.next_hedge_retry_ms} "
                                    f"deadline_ms={int(pending.deadline_ms)}"
                                ),
                            )
                            return
                    else:
                        if now_ms < int(pending.deadline_ms):
                            return

                if target_hit and effective_leg2_mode not in {"passive_limit", "force_unwind"}:
                    if now_ms < int(pending.next_hedge_retry_ms):
                        return
                    if not pending.leg2_token_id:
                        raise ValueError("missing_leg2_token_id")
                    hedge_size = float(
                        self._leg2_size_with_min_notional(
                            leg2_price=float(leg2_ask),
                            leg1_filled_qty=float(pending.quantity),
                        )
                    )
                    if hedge_size <= EPS:
                        raise ValueError("invalid_leg2_size")
                    self._emit_console_action(
                        ts_utc=str(snapshot["ts_utc"]),
                        market_key=market_key,
                        action="try_hedge",
                        reason_code="",
                        leg1_side=pending.leg1_side,
                        leg1_price=pending.leg1_ask,
                        leg2_price=float(leg2_ask),
                        unwind_price=None,
                        order_id_leg1=pending.order_id_leg1,
                        order_id_leg2=pending.order_id_leg2,
                        note="",
                    )
                    hedge_result = self.executor.place_leg2_hedge(  # type: ignore[union-attr]
                        token_id=pending.leg2_token_id,
                        price=float(leg2_ask),
                        size=float(hedge_size),
                        timeout_ms=max(300, min(3000, int(phase_remaining_ms))),
                        order_type=str(self.cfg.leg2_order_type),
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
                            note=f"live_hedge_success target_b<={leg2_target:.6f}",
                        )
                        return

                    pending.fill_status_leg2 = hedge_result.status or "hedge_failed"
                    if now_ms < int(pending.deadline_ms):
                        retry_wait_ms = max(700, int(float(self.cfg.poll_interval_sec) * 1000))
                        pending.next_hedge_retry_ms = int(now_ms + retry_wait_ms)
                        self._append_action(
                            ts_utc=snapshot["ts_utc"],
                            market_key=market_key,
                            action="try_hedge",
                            status="retrying",
                            reason_code="hedge_retry_pending",
                            leg1_side=pending.leg1_side,
                            yes_ask=yes_ask,
                            no_ask=no_ask,
                            sum_ask=sum_ask,
                            seconds_to_close=snapshot["seconds_to_close"],
                            leg1_price=pending.leg1_ask,
                            leg2_price=float(leg2_ask),
                            order_id_leg1=pending.order_id_leg1,
                            order_id_leg2=pending.order_id_leg2,
                            fill_status_leg1=pending.fill_status_leg1 or "submitted",
                            fill_status_leg2=pending.fill_status_leg2,
                            hedge_latency_ms=hedge_result.latency_ms,
                            unwind_triggered=False,
                            note=(
                                f"phase={phase_regime} "
                                f"hedge_error={hedge_result.error or hedge_result.status}; "
                                f"retry_at_ms={pending.next_hedge_retry_ms} deadline_ms={int(pending.deadline_ms)}"
                            ),
                        )
                        return

                    pending.leg2_mode = "force_unwind"
                    unwind_price_hint = _safe_float(unwind_bid, default=pending.leg1_ask) or float(pending.leg1_ask)
                    if not pending.leg1_token_id:
                        raise ValueError("missing_leg1_token_id")
                    unwind_qty = self._resolve_unwind_quantity(pending=pending)
                    if unwind_qty <= EPS:
                        self.trades_closed_by_market[market_key] += 1
                        self.pending_by_market.pop(market_key, None)
                        self._append_action(
                            ts_utc=snapshot["ts_utc"],
                            market_key=market_key,
                            action="unwind_leg1",
                            status="closed",
                            reason_code="leg1_not_filled_no_inventory",
                            leg1_side=pending.leg1_side,
                            yes_ask=yes_ask,
                            no_ask=no_ask,
                            sum_ask=sum_ask,
                            seconds_to_close=snapshot["seconds_to_close"],
                            leg1_price=pending.leg1_ask,
                            order_id_leg1=pending.order_id_leg1,
                            order_id_leg2=pending.order_id_leg2,
                            fill_status_leg1=pending.fill_status_leg1 or "submitted",
                            fill_status_leg2=hedge_result.status or pending.fill_status_leg2,
                            unwind_triggered=True,
                            note=f"hedge_error={hedge_result.error or hedge_result.status}; no_inventory_to_unwind",
                        )
                        return
                    unwind_result = self.executor.unwind_leg1(  # type: ignore[union-attr]
                        token_id=pending.leg1_token_id,
                        price=float(unwind_price_hint),
                        size=float(unwind_qty),
                        order_type=str(self.cfg.unwind_order_type),
                        timeout_ms=int(self.cfg.leg2_timeout_ms),
                    )
                    pending.fill_status_leg2 = hedge_result.status or "hedge_failed"
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

                    unwind_error = str(unwind_result.error or unwind_result.status or "")
                    if _is_balance_allowance_error(unwind_error):
                        retry_result = self._retry_unwind_on_balance_error(
                            pending=pending,
                            unwind_price_hint=float(unwind_price_hint),
                            leg2_ask=float(leg2_ask),
                            leg2_target=float(leg2_target),
                            context_note=f"hedge_error={hedge_result.error or hedge_result.status}",
                        )
                        if retry_result.success:
                            unwind_price = (
                                retry_result.avg_price if retry_result.avg_price > 0 else float(unwind_price_hint)
                            )
                            pnl_delta = (float(unwind_price) - float(pending.leg1_ask)) * float(self.cfg.stake_usd_per_leg)
                            self.daily_pnl += float(pnl_delta)
                            self.trades_closed_by_market[market_key] += 1
                            self.pending_by_market.pop(market_key, None)
                            self._append_action(
                                ts_utc=snapshot["ts_utc"],
                                market_key=market_key,
                                action="unwind_leg1",
                                status="closed",
                                reason_code="leg2_failed_unwind_retry",
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
                                note="live_hedge_failed_unwind_retry_success",
                            )
                            return
                        retry_error = str(retry_result.error or retry_result.status or "")
                        if _is_balance_allowance_error(retry_error):
                            self._mark_unwind_pending(
                                snapshot=snapshot,
                                pending=pending,
                                reason_code="unwind_balance_retry_pending",
                                note=(
                                    f"hedge_error={hedge_result.error or hedge_result.status}; "
                                    f"unwind_error={retry_error}"
                                ),
                            )
                            return
                        self._mark_unwind_pending(
                            snapshot=snapshot,
                            pending=pending,
                            reason_code="unwind_retry_pending",
                            note=(
                                f"hedge_error={hedge_result.error or hedge_result.status}; "
                                f"unwind_error={retry_error}"
                            ),
                        )
                        return

                    self._mark_unwind_pending(
                        snapshot=snapshot,
                        pending=pending,
                        reason_code="unwind_pending",
                        note=(
                            f"hedge_error={hedge_result.error or hedge_result.status}; "
                            f"unwind_error={unwind_result.error or unwind_result.status}"
                        ),
                    )
                    return
                if now_ms < int(pending.deadline_ms):
                    return

                pending.leg2_mode = "force_unwind"
                unwind_price_hint = _safe_float(unwind_bid, default=pending.leg1_ask) or float(pending.leg1_ask)
                if not pending.leg1_token_id:
                    raise ValueError("missing_leg1_token_id")
                unwind_qty = self._resolve_unwind_quantity(pending=pending)
                if unwind_qty <= EPS:
                    self.trades_closed_by_market[market_key] += 1
                    self.pending_by_market.pop(market_key, None)
                    self._append_action(
                        ts_utc=snapshot["ts_utc"],
                        market_key=market_key,
                        action="unwind_leg1",
                        status="closed",
                        reason_code="leg2_timeout_no_inventory",
                        leg1_side=pending.leg1_side,
                        yes_ask=yes_ask,
                        no_ask=no_ask,
                        sum_ask=sum_ask,
                        seconds_to_close=snapshot["seconds_to_close"],
                        leg1_price=pending.leg1_ask,
                        order_id_leg1=pending.order_id_leg1,
                        order_id_leg2=pending.order_id_leg2,
                        fill_status_leg1=pending.fill_status_leg1 or "submitted",
                        fill_status_leg2=pending.fill_status_leg2,
                        unwind_triggered=True,
                        note="target_not_reached; no_inventory_to_unwind",
                    )
                    return
                unwind_result = self.executor.unwind_leg1(  # type: ignore[union-attr]
                    token_id=pending.leg1_token_id,
                    price=float(unwind_price_hint),
                    size=float(unwind_qty),
                    order_type=str(self.cfg.unwind_order_type),
                    timeout_ms=int(self.cfg.leg2_timeout_ms),
                )
                pending.fill_status_leg2 = "target_not_reached"
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
                        reason_code="leg2_timeout_unwind",
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
                        hedge_latency_ms=0,
                        unwind_triggered=True,
                        pnl_delta=pnl_delta,
                        note=f"target_not_reached leg2_ask={leg2_ask:.6f} target_b<={leg2_target:.6f}",
                    )
                    return

                unwind_error = str(unwind_result.error or unwind_result.status or "")
                if _is_balance_allowance_error(unwind_error):
                    retry_result = self._retry_unwind_on_balance_error(
                        pending=pending,
                        unwind_price_hint=float(unwind_price_hint),
                        leg2_ask=float(leg2_ask),
                        leg2_target=float(leg2_target),
                        context_note="target_not_reached",
                    )
                    if retry_result.success:
                        unwind_price = retry_result.avg_price if retry_result.avg_price > 0 else float(unwind_price_hint)
                        pnl_delta = (float(unwind_price) - float(pending.leg1_ask)) * float(self.cfg.stake_usd_per_leg)
                        self.daily_pnl += float(pnl_delta)
                        self.trades_closed_by_market[market_key] += 1
                        self.pending_by_market.pop(market_key, None)
                        self._append_action(
                            ts_utc=snapshot["ts_utc"],
                            market_key=market_key,
                            action="unwind_leg1",
                            status="closed",
                            reason_code="leg2_timeout_unwind_retry",
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
                            hedge_latency_ms=0,
                            unwind_triggered=True,
                            pnl_delta=pnl_delta,
                            note="target_not_reached_unwind_retry_success",
                        )
                        return
                    retry_error = str(retry_result.error or retry_result.status or "")
                    if _is_balance_allowance_error(retry_error):
                        self._mark_unwind_pending(
                            snapshot=snapshot,
                            pending=pending,
                            reason_code="unwind_balance_retry_pending",
                            note=(
                                f"target_not_reached leg2_ask={leg2_ask:.6f} target_b<={leg2_target:.6f}; "
                                f"unwind_error={retry_error}"
                            ),
                        )
                        return
                    self._mark_unwind_pending(
                        snapshot=snapshot,
                        pending=pending,
                        reason_code="unwind_retry_pending",
                        note=(
                            f"target_not_reached leg2_ask={leg2_ask:.6f} target_b<={leg2_target:.6f}; "
                            f"unwind_error={retry_error}"
                        ),
                    )
                    return

                self._mark_unwind_pending(
                    snapshot=snapshot,
                    pending=pending,
                    reason_code="unwind_pending",
                    note=(
                        f"target_not_reached leg2_ask={leg2_ask:.6f} target_b<={leg2_target:.6f}; "
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

        if target_hit:
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
                fill_status_leg2="simulated_fill_target",
                hedge_latency_ms=0,
                unwind_triggered=False,
                pnl_delta=pnl_delta,
                note=f"mvp_hedge_success target_b<={leg2_target:.6f}",
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
        reason_code = "leg2_timeout_unwind"
        note = f"mvp_timeout_unwind target_not_reached leg2_ask={leg2_ask:.6f} target_b<={leg2_target:.6f}"
        if unwind_loss_bps > float(self.cfg.max_unwind_loss_bps) + EPS:
            reason_code = "leg2_timeout_unwind_over_limit"
            note = (
                f"mvp_timeout_unwind target_not_reached leg2_ask={leg2_ask:.6f} "
                f"target_b<={leg2_target:.6f} loss_bps={unwind_loss_bps:.2f} "
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

    def _open_leg1(
        self,
        *,
        snapshot: dict[str, Any],
        edge_regime: str,
        target_profit_pct: float,
    ) -> None:
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
        leg2_target_max_ask = self._leg2_target_max_ask(entry_ask=leg1_ask, target_profit_pct=target_profit_pct)
        leg_quantity = self._size_for_leg(leg1_ask)
        leg_quantity_exec = float(leg_quantity)
        order_id_leg1 = ""
        fill_status_leg1 = "simulated_open"
        high_ms = int(self.cfg.leg2_timeout_high_ms)
        mid_ms = int(self.cfg.leg2_timeout_mid_ms)
        weak_ms = int(self.cfg.leg2_timeout_weak_ms)
        total_ladder_ms = int(high_ms + mid_ms + weak_ms)
        phase_high_deadline_ms = int(now_ms + high_ms)
        phase_mid_deadline_ms = int(phase_high_deadline_ms + mid_ms)
        phase_weak_deadline_ms = int(phase_mid_deadline_ms + weak_ms)

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
                self._emit_console_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="try_leg1",
                    reason_code="",
                    leg1_side=leg1_side,
                    leg1_price=leg1_ask,
                    leg2_price=None,
                    unwind_price=None,
                    order_id_leg1="",
                    order_id_leg2="",
                    note="",
                )
                leg1_result = self.executor.place_leg1_order(  # type: ignore[union-attr]
                    token_id=leg1_token_id,
                    price=float(leg1_ask),
                    size=float(leg_quantity),
                    order_type=str(self.cfg.leg1_order_type),
                    timeout_ms=min(1500, int(high_ms)),
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
            matched_qty = float(leg1_result.filled_size)
            if matched_qty > EPS:
                leg_quantity_exec = matched_qty
            if leg_quantity_exec <= EPS:
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
                    fill_status_leg1=fill_status_leg1,
                    note="leg1_error=filled_size_zero",
                )
                return

        pending = PendingTrade(
            market_key=market_key,
            opened_ts_utc=ts_utc,
            opened_at_ms=now_ms,
            deadline_ms=phase_weak_deadline_ms,
            phase_high_deadline_ms=phase_high_deadline_ms,
            phase_mid_deadline_ms=phase_mid_deadline_ms,
            phase_weak_deadline_ms=phase_weak_deadline_ms,
            leg1_side=leg1_side,
            leg1_ask=float(leg1_ask),
            leg1_bid=None if leg1_bid is None else float(leg1_bid),
            leg2_target_max_ask=float(leg2_target_max_ask),
            sum_ask_entry=float(snapshot["sum_ask"]),
            edge_regime=str(edge_regime),
            leg2_mode="aggressive",
            leg1_token_id=leg1_token_id,
            leg2_token_id=leg2_token_id,
            quantity=float(leg_quantity_exec),
            order_id_leg1=order_id_leg1,
            fill_status_leg1=fill_status_leg1,
        )
        self.pending_by_market[market_key] = pending
        self.trades_opened_by_market[market_key] += 1
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
            leg2_price=leg2_target_max_ask,
            order_id_leg1=order_id_leg1,
            fill_status_leg1=fill_status_leg1,
            unwind_triggered=False,
            note=(
                f"{'mvp_entry_live' if self._live_enabled() else 'mvp_entry'} "
                f"entry_regime={edge_regime} "
                f"ladder_ms=(high={high_ms},mid={mid_ms},weak={weak_ms},total={total_ladder_ms}) "
                f"target_profit_pct={target_profit_pct:.4f} "
                f"target_b<={leg2_target_max_ask:.6f} "
                f"qty_req={leg_quantity:.6f} qty_exec={leg_quantity_exec:.6f}"
            ),
        )

    def _result_is_filled(self, result: LiveOrderResult) -> bool:
        status_txt = str(result.status or "").strip().lower()
        filled_qty = float(result.filled_size or 0.0)
        positive_status = {"filled", "matched", "partially_filled", "partiallyfilled"}
        return bool(result.success and (filled_qty > EPS or status_txt in positive_status))

    def _open_dual_fok(self, *, snapshot: dict[str, Any], edge_regime: str) -> None:
        market_key = str(snapshot["market_key"])
        ts_utc = str(snapshot["ts_utc"])
        yes_ask = float(snapshot["yes_ask"])
        no_ask = float(snapshot["no_ask"])
        yes_bid = _safe_float(snapshot.get("yes_bid"), default=None)
        no_bid = _safe_float(snapshot.get("no_bid"), default=None)
        token_up = str(snapshot.get("token_up", "")).strip()
        token_down = str(snapshot.get("token_down", "")).strip()
        if not token_up or not token_down:
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_missing_tokens",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=float(snapshot["sum_ask"]),
                seconds_to_close=int(snapshot["seconds_to_close"]),
                note=f"token_up={bool(token_up)} token_down={bool(token_down)}",
            )
            return

        dual_qty = max(float(self._size_for_leg(yes_ask)), float(self._size_for_leg(no_ask)))
        timeout_ms = max(300, int(self.cfg.dual_fok_timeout_ms))
        self._append_action(
            ts_utc=ts_utc,
            market_key=market_key,
            action="try_leg1",
            status="open",
            reason_code="dual_fok_submit",
            leg1_side="both",
            yes_ask=yes_ask,
            no_ask=no_ask,
            sum_ask=float(snapshot["sum_ask"]),
            seconds_to_close=int(snapshot["seconds_to_close"]),
            leg1_price=min(yes_ask, no_ask),
            note=f"edge_regime={edge_regime} dual_qty={dual_qty:.6f} timeout_ms={timeout_ms}",
        )

        up_result: LiveOrderResult
        down_result: LiveOrderResult
        if self._live_enabled():
            try:
                assert self.executor is not None
                with concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="dual-fok-entry") as pool:
                    up_future = pool.submit(
                        self.executor.place_leg1_order,
                        token_id=token_up,
                        price=float(yes_ask),
                        size=float(dual_qty),
                        order_type="FOK",
                        timeout_ms=int(timeout_ms),
                    )
                    down_future = pool.submit(
                        self.executor.place_leg1_order,
                        token_id=token_down,
                        price=float(no_ask),
                        size=float(dual_qty),
                        order_type="FOK",
                        timeout_ms=int(timeout_ms),
                    )
                    up_result = up_future.result()
                    down_result = down_future.result()
            except Exception as exc:  # noqa: BLE001
                self._trigger_kill_switch(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    reason_code="kill_switch_dual_fok_submit_error",
                    note=f"exception={exc}",
                )
                return
        else:
            up_result = LiveOrderResult(
                success=True,
                order_id=f"dry_yes_{int(time.time() * 1000)}",
                status="filled",
                filled_size=float(dual_qty),
                avg_price=float(yes_ask),
                latency_ms=0,
            )
            down_result = LiveOrderResult(
                success=True,
                order_id=f"dry_no_{int(time.time() * 1000)}",
                status="filled",
                filled_size=float(dual_qty),
                avg_price=float(no_ask),
                latency_ms=0,
            )

        yes_filled = self._result_is_filled(up_result)
        no_filled = self._result_is_filled(down_result)

        if yes_filled and no_filled:
            qty_exec = min(
                float(up_result.filled_size or dual_qty),
                float(down_result.filled_size or dual_qty),
            )
            qty_exec = max(EPS, qty_exec)
            pnl_delta = (1.0 - float(yes_ask) - float(no_ask)) * float(qty_exec)
            self.daily_pnl += float(pnl_delta)
            self.trades_opened_by_market[market_key] += 1
            self.trades_closed_by_market[market_key] += 1
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="hedge_leg2",
                status="closed",
                reason_code="entered_dual_fok",
                leg1_side="both",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=float(snapshot["sum_ask"]),
                seconds_to_close=int(snapshot["seconds_to_close"]),
                leg1_price=float(yes_ask),
                leg2_price=float(no_ask),
                order_id_leg1=up_result.order_id,
                order_id_leg2=down_result.order_id,
                fill_status_leg1=up_result.status,
                fill_status_leg2=down_result.status,
                hedge_latency_ms=max(int(up_result.latency_ms), int(down_result.latency_ms)),
                unwind_triggered=False,
                pnl_delta=pnl_delta,
                note=f"dual_fok_both_filled qty={qty_exec:.6f}",
            )
            return

        if (not yes_filled) and (not no_filled):
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_dual_fok_rejected",
                leg1_side="both",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=float(snapshot["sum_ask"]),
                seconds_to_close=int(snapshot["seconds_to_close"]),
                fill_status_leg1=up_result.status,
                fill_status_leg2=down_result.status,
                note=(
                    f"yes_error={up_result.error or up_result.status}; "
                    f"no_error={down_result.error or down_result.status}; "
                    f"dual_qty={dual_qty:.6f}"
                ),
            )
            return

        filled_side = "yes" if yes_filled else "no"
        filled_result = up_result if yes_filled else down_result
        failed_result = down_result if yes_filled else up_result
        filled_token = token_up if yes_filled else token_down
        filled_entry_price = float(yes_ask if yes_filled else no_ask)
        unwind_price_hint = _safe_float(yes_bid if yes_filled else no_bid, default=None)
        if unwind_price_hint is None:
            unwind_price_hint = float(filled_entry_price)
        unwind_qty = max(EPS, float(filled_result.filled_size or dual_qty))

        if self._live_enabled():
            assert self.executor is not None
            unwind_result = self.executor.unwind_leg1(
                token_id=filled_token,
                price=float(unwind_price_hint),
                size=float(unwind_qty),
                order_type=str(self.cfg.unwind_order_type),
                timeout_ms=int(self.cfg.leg2_timeout_ms),
            )
        else:
            unwind_result = LiveOrderResult(
                success=True,
                order_id=f"dry_unwind_{int(time.time() * 1000)}",
                status="filled",
                filled_size=float(unwind_qty),
                avg_price=float(unwind_price_hint),
                latency_ms=0,
            )

        if (
            (not unwind_result.success)
            and self._live_enabled()
            and self.executor is not None
            and _is_balance_allowance_error(str(unwind_result.error or unwind_result.status or ""))
        ):
            retry_deadline = time.time() + 6.0
            while time.time() < retry_deadline and not unwind_result.success:
                time.sleep(0.8)
                available = float(self.executor.get_token_available_balance(filled_token))
                retry_qty = min(float(unwind_qty), max(0.0, available * 0.998))
                if retry_qty <= EPS:
                    continue
                retry_result = self.executor.unwind_leg1(
                    token_id=filled_token,
                    price=float(unwind_price_hint),
                    size=float(retry_qty),
                    order_type=str(self.cfg.unwind_order_type),
                    timeout_ms=int(self.cfg.leg2_timeout_ms),
                )
                if retry_result.success:
                    unwind_qty = float(retry_qty)
                    unwind_result = retry_result
                    break
                unwind_result = retry_result

        if unwind_result.success:
            unwind_price = float(unwind_result.avg_price if unwind_result.avg_price > 0 else unwind_price_hint)
            pnl_delta = (float(unwind_price) - float(filled_entry_price)) * float(unwind_qty)
            self.daily_pnl += float(pnl_delta)
            self.trades_opened_by_market[market_key] += 1
            self.trades_closed_by_market[market_key] += 1
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="unwind_leg1",
                status="closed",
                reason_code="dual_fok_single_fill_unwind",
                leg1_side=filled_side,
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=float(snapshot["sum_ask"]),
                seconds_to_close=int(snapshot["seconds_to_close"]),
                leg1_price=float(filled_entry_price),
                unwind_price=float(unwind_price),
                order_id_leg1=filled_result.order_id,
                order_id_leg2=failed_result.order_id,
                fill_status_leg1=filled_result.status,
                fill_status_leg2=failed_result.status,
                hedge_latency_ms=max(int(filled_result.latency_ms), int(failed_result.latency_ms)),
                unwind_triggered=True,
                pnl_delta=pnl_delta,
                note=(
                    f"leg2_fail_reason={failed_result.error or failed_result.status}; "
                    f"unwind_result={unwind_result.status} qty={unwind_qty:.6f}"
                ),
            )
            return

        unwind_error = str(unwind_result.error or unwind_result.status or "unwind_failed")
        self._append_action(
            ts_utc=ts_utc,
            market_key=market_key,
            action="unwind_leg1",
            status="blocked",
            reason_code="dual_fok_unwind_failed",
            leg1_side=filled_side,
            yes_ask=yes_ask,
            no_ask=no_ask,
            sum_ask=float(snapshot["sum_ask"]),
            seconds_to_close=int(snapshot["seconds_to_close"]),
            leg1_price=float(filled_entry_price),
            unwind_price=float(unwind_price_hint),
            order_id_leg1=filled_result.order_id,
            order_id_leg2=failed_result.order_id,
            fill_status_leg1=filled_result.status,
            fill_status_leg2=failed_result.status,
            unwind_triggered=True,
            note=(
                f"leg2_fail_reason={failed_result.error or failed_result.status}; "
                f"unwind_result={unwind_error} qty={unwind_qty:.6f}"
            ),
        )
        self._trigger_kill_switch(
            ts_utc=ts_utc,
            market_key=market_key,
            reason_code="kill_switch_dual_fok_unwind_failed",
            note=(
                f"leg2_fail_reason={failed_result.error or failed_result.status}; "
                f"unwind_result={unwind_error} qty={unwind_qty:.6f}"
            ),
        )

    def _process_snapshot(self, snapshot: dict[str, Any]) -> None:
        market_key = str(snapshot["market_key"])
        ts_utc = str(snapshot["ts_utc"])
        self._active_feed_source = str(snapshot.get("feed_source", "")).strip()
        yes_ask = float(snapshot["yes_ask"])
        no_ask = float(snapshot["no_ask"])
        sum_ask = float(snapshot["sum_ask"])
        seconds_to_close = int(snapshot["seconds_to_close"])
        snap_sig = (
            f"{round(float(yes_ask), 6)}|{round(float(no_ask), 6)}|"
            f"{round(float(_safe_float(snapshot.get('yes_bid'), default=0.0) or 0.0), 6)}|"
            f"{round(float(_safe_float(snapshot.get('no_bid'), default=0.0) or 0.0), 6)}|"
            f"{round(float(_safe_float(snapshot.get('yes_ask_size'), default=0.0) or 0.0), 6)}|"
            f"{round(float(_safe_float(snapshot.get('no_ask_size'), default=0.0) or 0.0), 6)}|"
            f"{int(seconds_to_close)}"
        )
        prev_sig = self._last_snapshot_hash_by_market.get(market_key)
        if prev_sig == snap_sig:
            self._record_skip_unchanged()
            return
        self._last_snapshot_hash_by_market[market_key] = snap_sig
        self._last_book_snapshot = {
            "ts_utc": ts_utc,
            "market_key": market_key,
            "yes_bid": _safe_float(snapshot.get("yes_bid"), default=None),
            "yes_ask": yes_ask,
            "yes_ask_size": _safe_float(snapshot.get("yes_ask_size"), default=0.0) or 0.0,
            "no_bid": _safe_float(snapshot.get("no_bid"), default=None),
            "no_ask": no_ask,
            "no_ask_size": _safe_float(snapshot.get("no_ask_size"), default=0.0) or 0.0,
            "sum_ask": sum_ask,
            "seconds_to_close": seconds_to_close,
        }

        existing = self.pending_by_market.get(market_key)
        if existing is not None:
            self._attempt_hedge_or_unwind(snapshot=snapshot, pending=existing)
            return

        data_age_ms = _safe_float(snapshot.get("data_age_ms"), default=None)
        if data_age_ms is not None and float(data_age_ms) > float(self.cfg.api_max_data_freshness_ms):
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_stale_snapshot",
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=(
                    f"data_age_ms={float(data_age_ms):.1f} > "
                    f"api_max_data_freshness_ms={int(self.cfg.api_max_data_freshness_ms)} "
                    f"feed_source={self._active_feed_source or '-'}"
                ),
            )
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

        opened_count = int(self.trades_opened_by_market.get(market_key, 0))
        if opened_count >= int(self.cfg.max_trades_per_market):
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
                    f"trades_opened={opened_count} "
                    f"trades_closed={int(self.trades_closed_by_market.get(market_key, 0))} "
                    f">= max_trades_per_market={int(self.cfg.max_trades_per_market)}"
                ),
            )
            return

        stable_ticks = self._update_book_stability(snapshot=snapshot)
        edge_regime = self._classify_edge_regime(sum_ask=sum_ask)
        regime_for_leg2 = edge_regime
        if edge_regime == "NO_TRADE":
            # Delay arb: do not block leg1 by sum_ask.
            # Treat as weakest regime for leg2 behavior/timeouts.
            regime_for_leg2 = "LOW_EDGE"
        target_regime = edge_regime if bool(self.cfg.dual_fok_enabled) else regime_for_leg2
        target_profit_pct = self._target_profit_pct_for_regime(regime=target_regime)
        leg1_side, leg1_ask, leg1_bid, _ = self._pick_leg1(snapshot=snapshot)
        yes_spread = _safe_float(snapshot.get("yes_spread"), default=None)
        no_spread = _safe_float(snapshot.get("no_spread"), default=None)
        leg1_spread = _safe_float(snapshot.get("yes_spread"), default=None) if leg1_side == "yes" else _safe_float(
            snapshot.get("no_spread"), default=None
        )
        yes_ask_size = _safe_float(snapshot.get("yes_ask_size"), default=0.0)
        no_ask_size = _safe_float(snapshot.get("no_ask_size"), default=0.0)
        required_yes_qty = float(self._size_for_leg(yes_ask))
        required_no_qty = float(self._size_for_leg(no_ask))
        required_leg1_qty = required_yes_qty if leg1_side == "yes" else required_no_qty
        required_dual_qty = max(required_yes_qty, required_no_qty)
        leg1_size = (
            yes_ask_size
            if leg1_side == "yes"
            else no_ask_size
        )

        if bool(self.cfg.dual_fok_enabled):
            required_sum_ask = 1.0 - (float(target_profit_pct) / 100.0)
            if float(sum_ask) > float(required_sum_ask) + EPS:
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_target_profit_floor",
                    leg1_side=leg1_side,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    sum_ask=sum_ask,
                    seconds_to_close=seconds_to_close,
                    note=(
                        f"sum_ask={sum_ask:.6f} required_sum_ask<={required_sum_ask:.6f} "
                        f"target_profit_pct={target_profit_pct:.4f}"
                    ),
                )
                return
        if not self._within_bounds(leg1_ask):
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_leg1_price_bounds",
                leg1_side=leg1_side,
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=f"leg1_ask={leg1_ask:.6f} required_range=[{self.cfg.price_min:.2f},{self.cfg.price_max:.2f}]",
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

        if bool(self.cfg.dual_fok_enabled):
            if (not self._within_spread(yes_spread)) or (not self._within_spread(no_spread)):
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_dual_spread",
                    leg1_side=leg1_side,
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
            if (not self._within_depth(yes_ask_size, required_qty=required_dual_qty)) or (
                not self._within_depth(no_ask_size, required_qty=required_dual_qty)
            ):
                needed = float(required_dual_qty) * float(self.cfg.min_depth_buffer_mult)
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_dual_depth",
                    leg1_side=leg1_side,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    sum_ask=sum_ask,
                    seconds_to_close=seconds_to_close,
                    note=(
                        f"required_dual_qty={required_dual_qty:.6f} needed_per_side={needed:.6f} "
                        f"yes_size={yes_ask_size} no_size={no_ask_size}"
                    ),
                )
                return
        else:
            if not self._within_spread(leg1_spread):
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_leg1_spread",
                    leg1_side=leg1_side,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    sum_ask=sum_ask,
                    seconds_to_close=seconds_to_close,
                    note=(
                        f"leg1_spread={leg1_spread} max_spread={float(self.cfg.max_spread):.6f} "
                        f"leg1_side={leg1_side}"
                    ),
                )
                return

            if not self._within_depth(leg1_size, required_qty=required_leg1_qty):
                needed = float(required_leg1_qty) * float(self.cfg.min_depth_buffer_mult)
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_leg1_depth",
                    leg1_side=leg1_side,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    sum_ask=sum_ask,
                    seconds_to_close=seconds_to_close,
                    note=(
                        f"leg1_size={leg1_size} needed={needed:.6f} "
                        f"required_leg1_qty={required_leg1_qty:.6f} leg1_side={leg1_side}"
                    ),
                )
                return

        if edge_regime == "MID_EDGE":
            if (not self._within_depth(yes_ask_size, required_qty=required_yes_qty)) or (
                not self._within_depth(no_ask_size, required_qty=required_no_qty)
            ):
                needed_yes = float(required_yes_qty) * float(self.cfg.min_depth_buffer_mult)
                needed_no = float(required_no_qty) * float(self.cfg.min_depth_buffer_mult)
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_mid_edge_depth",
                    leg1_side=leg1_side,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    sum_ask=sum_ask,
                    seconds_to_close=seconds_to_close,
                    note=(
                        f"mid_edge yes_size={yes_ask_size} needed_yes={needed_yes:.6f}; "
                        f"no_size={no_ask_size} needed_no={needed_no:.6f}"
                    ),
                )
                return
            if int(stable_ticks) < int(self.cfg.mid_edge_stable_ticks):
                self._append_action(
                    ts_utc=ts_utc,
                    market_key=market_key,
                    action="skip_entry",
                    status="blocked",
                    reason_code="blocked_mid_edge_book_unstable",
                    leg1_side=leg1_side,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    sum_ask=sum_ask,
                    seconds_to_close=seconds_to_close,
                    note=f"stable_ticks={stable_ticks} < required={int(self.cfg.mid_edge_stable_ticks)}",
                )
                return

        leg2_target_max_ask = self._leg2_target_max_ask(
            entry_ask=leg1_ask,
            target_profit_pct=target_profit_pct,
        )
        if float(leg2_target_max_ask) <= EPS:
            self._append_action(
                ts_utc=ts_utc,
                market_key=market_key,
                action="skip_entry",
                status="blocked",
                reason_code="blocked_target_unreachable",
                leg1_side=leg1_side,
                yes_ask=yes_ask,
                no_ask=no_ask,
                sum_ask=sum_ask,
                seconds_to_close=seconds_to_close,
                note=(
                    f"leg1_ask={leg1_ask:.6f} target_profit_pct={target_profit_pct:.4f} "
                    f"target_b<={leg2_target_max_ask:.6f}"
                ),
            )
            return

        if bool(self.cfg.dual_fok_enabled):
            self._open_dual_fok(
                snapshot=snapshot,
                edge_regime=regime_for_leg2,
            )
            return

        self._open_leg1(
            snapshot=snapshot,
            edge_regime=regime_for_leg2,
            target_profit_pct=target_profit_pct,
        )
        pending = self.pending_by_market.get(market_key)
        if pending is not None:
            self._attempt_hedge_or_unwind(snapshot=snapshot, pending=pending)

    def run(self) -> int:
        mode = "live" if self._live_enabled() else ("dry-run" if self.dry_run else "paper")
        target_label = (
            f"{self.cfg.target_profit_pct}"
            if abs(float(self.cfg.target_profit_max_pct) - float(self.cfg.target_profit_pct)) <= EPS
            else f"{self.cfg.target_profit_pct}-{self.cfg.target_profit_max_pct}"
        )
        print(
            f"[live-mvp] start mode={mode} market={self.cfg.market_scope} stake={self.cfg.stake_usd_per_leg} "
            f"target_profit_pct={target_label} timeout_ms={self.cfg.leg2_timeout_ms} "
            f"max_daily_loss_usd={self.cfg.max_daily_loss_usd} "
            f"cutoff_sec={self.cfg.entry_cutoff_sec} max_spread={self.cfg.max_spread} "
            f"depth_mult={self.cfg.min_depth_buffer_mult} max_unwind_loss_bps={self.cfg.max_unwind_loss_bps} "
            f"max_trades_per_market={self.cfg.max_trades_per_market} "
            f"order_types=(leg1={self.cfg.leg1_order_type},leg2={self.cfg.leg2_order_type},unwind={self.cfg.unwind_order_type}) "
            f"dual_fok_enabled={bool(self.cfg.dual_fok_enabled)} dual_fok_timeout_ms={int(self.cfg.dual_fok_timeout_ms)} "
            f"regime_edges=(high<={self.cfg.edge_high_max_sum_ask},mid<={self.cfg.edge_mid_max_sum_ask},"
            f"low<={self.cfg.edge_weak_max_sum_ask}) "
            f"regime_timeouts_ms=(high={self.cfg.leg2_timeout_high_ms},mid={self.cfg.leg2_timeout_mid_ms},low={self.cfg.leg2_timeout_weak_ms})"
        )
        print(
            f"[live-mvp] market_source={self.market_source} raw_dir={self.raw_dir} "
            f"report_file={self.report_file} show_orderbook={self.show_orderbook} "
            f"terminal_dashboard={self.terminal_dashboard} dashboard_rows={self.dashboard_rows}"
        )
        if self.market_source == "api":
            manual_count = len(self._manual_api_slugs(now_utc=datetime.now(timezone.utc)))
            print(
                f"[live-mvp] api_depth={int(self.cfg.api_book_depth)} "
                f"api_markets_count={int(self.cfg.api_markets_count)} "
                f"api_manual_markets={manual_count} "
                f"api_max_markets_per_cycle={int(self.cfg.api_max_markets_per_cycle)} "
                f"api_max_data_freshness_ms={int(self.cfg.api_max_data_freshness_ms)} "
                f"api_top_interval_ms={int(self.cfg.api_top_interval_ms)} "
                f"api_book_interval_ms={int(self.cfg.api_book_interval_ms)} "
                f"api_trades_interval_ms={int(self.cfg.api_trades_interval_ms)} "
                f"api_request_timeout_ms={int(self.cfg.api_request_timeout_ms)} "
                f"api_request_retries={int(self.cfg.api_request_retries)} "
                f"api_request_backoff_ms={int(self.cfg.api_request_backoff_ms)} "
                f"api_effective_quotes={bool(self.cfg.api_use_effective_quotes)} "
                f"api_skip_unchanged_book={bool(self.cfg.api_skip_unchanged_book)} "
                f"api_backoff_on_error={bool(self.cfg.api_backoff_on_error)} "
                f"api_max_rps_guard={float(self.cfg.api_max_rps_guard):.2f} "
                f"api_workers={int(self.cfg.api_parallel_workers)} "
                f"api_ws_enabled={bool(self.cfg.api_ws_enabled)} "
                f"api_ws_shards={int(self.cfg.api_ws_shards)} "
                f"api_ws_fallback_rest={bool(self.cfg.api_ws_fallback_rest)} "
                f"api_ws_stale_sec={float(self.cfg.api_ws_stale_sec):.3f} "
                f"api_prewarm_next_market={bool(self.cfg.api_prewarm_next_market)} "
                f"api_prewarm_sec={int(self.cfg.api_prewarm_sec)}"
            )
        if self.market_source == "file" and self.cfg.start_from_end:
            print("[live-mvp] start_from_end=true (ignora historico e processa apenas novos ticks)")
        if self.market_source == "api":
            self._warmup_api_state()
        started = time.time()
        try:
            while True:
                cycle_started = time.perf_counter()
                new_rows_total = 0
                if self.market_source == "api":
                    snapshots = self._read_api_snapshots()
                    new_rows_total = len(snapshots)
                    for snap in snapshots:
                        self.rows_processed += 1
                        data_age = _safe_float(snap.get("data_age_ms"), default=None)
                        if data_age is not None:
                            self._last_data_freshness_ms = float(max(0.0, data_age))
                        decision_started = time.perf_counter()
                        self._process_snapshot(snap)
                        self._current_decision_latency_ms = (time.perf_counter() - decision_started) * 1000.0
                        self._last_decision_latency_ms = float(max(0.0, self._current_decision_latency_ms))
                else:
                    self._bootstrap_offsets_from_end()
                    files = self._iter_orderbook_files()
                    for path in files:
                        rows = self._read_new_rows(path)
                        new_rows_total += len(rows)
                        for row in rows:
                            snap = self._extract_snapshot(row)
                            if snap is None:
                                continue
                            self.rows_processed += 1
                            self._last_data_freshness_ms = 0.0
                            decision_started = time.perf_counter()
                            self._process_snapshot(snap)
                            self._current_decision_latency_ms = (time.perf_counter() - decision_started) * 1000.0
                            self._last_decision_latency_ms = float(max(0.0, self._current_decision_latency_ms))

                self._last_loop_cycle_ms = (time.perf_counter() - cycle_started) * 1000.0

                if self.log_mode == "verbose":
                    print(
                        f"{_iso_utc_now()} | rows={new_rows_total} processed={self.rows_processed} "
                        f"open_pending={len(self.pending_by_market)} closed_trades={sum(self.trades_closed_by_market.values())} "
                        f"daily_pnl={self.daily_pnl:.6f} loop_cycle_ms={self._last_loop_cycle_ms:.1f} "
                        f"data_freshness_ms={self._last_data_freshness_ms:.1f} "
                        f"decision_latency_ms={self._last_decision_latency_ms:.1f}"
                    )
                else:
                    self._maybe_print_heartbeat()

                if self.kill_switch_triggered:
                    print(f"[live-mvp] kill-switch triggered reason={self.kill_switch_reason}")
                    break

                if self.runtime_sec > 0 and (time.time() - started) >= self.runtime_sec:
                    break
                time.sleep(float(self.cfg.poll_interval_sec))
        except KeyboardInterrupt:
            print("[live-mvp] interrupted by Ctrl+C")
        finally:
            self._flush_report_buffer_if_needed(force=True)
            if self._api_trades_future is not None and not self._api_trades_future.done():
                self._api_trades_future.cancel()
            self._api_trades_future = None
            if self._api_ws_feed is not None:
                self._api_ws_feed.stop()
                self._api_ws_feed = None
            if self._api_pool is not None:
                self._api_pool.shutdown(wait=False, cancel_futures=True)
                self._api_pool = None
            if self.api_collector is not None:
                try:
                    self.api_collector.close()
                except Exception:
                    pass

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
    parser.add_argument(
        "--max-trades-per-market",
        type=int,
        default=-1,
        help="Override do limite de trades por mercado (<=0 usa config).",
    )
    parser.add_argument("--raw-dir", default="", help="Optional override for config raw_dir")
    parser.add_argument("--report-file", default="", help="Optional override for config report_file")
    parser.add_argument("--enable-live", default="false", help="true|false (real orders only when true + confirm token)")
    parser.add_argument("--confirm-live", default="", help="Required token for live: I_UNDERSTAND_THE_RISK")
    parser.add_argument("--dry-run", default="true", help="true|false")
    parser.add_argument("--env-file", default=".env", help="Path to env file for live mode")
    parser.add_argument(
        "--market-source",
        default="file",
        choices=["file", "api"],
        help="file=usa orderbook_*.jsonl; api=consulta Polymarket direto",
    )
    parser.add_argument(
        "--collector-config",
        default="configs/data_collection_sol5m.json",
        help="Config de endpoints/slug para modo --market-source api",
    )
    parser.add_argument(
        "--log-mode",
        default="simple",
        choices=["simple", "verbose"],
        help="simple=eventos essenciais; verbose=heartbeat por ciclo",
    )
    parser.add_argument(
        "--heartbeat-sec",
        type=float,
        default=5.0,
        help="Intervalo do status 'buscando_sinal' no log-mode simple",
    )
    parser.add_argument(
        "--show-orderbook",
        default="false",
        help="true|false (mostra snapshot do best bid/ask no heartbeat do log simple)",
    )
    parser.add_argument(
        "--terminal-dashboard",
        default="false",
        help="true|false (painel ASCII com '|' exibindo metricas + ultimas entradas)",
    )
    parser.add_argument(
        "--dashboard-rows",
        type=int,
        default=8,
        help="Quantidade de linhas recentes no painel --terminal-dashboard (min 3).",
    )
    parser.add_argument(
        "--api-timeout-sec",
        type=float,
        default=-1.0,
        help="Override timeout das requests em --market-source api (<=0 usa config).",
    )
    parser.add_argument(
        "--api-retries",
        type=int,
        default=-1,
        help="Override retries das requests em --market-source api (<0 usa config).",
    )
    parser.add_argument(
        "--api-backoff-sec",
        type=float,
        default=-1.0,
        help="Override backoff das requests em --market-source api (<0 usa config).",
    )
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
    if int(args.max_trades_per_market) > 0:
        cfg = replace(cfg, max_trades_per_market=int(args.max_trades_per_market))
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
        if bool(cfg.preflight_on_start):
            preflight = executor.preflight()
            if not bool(preflight.get("ok")):
                print(f"[live-mvp] abort: preflight failed: {preflight}")
                return 2
            print(f"[live-mvp] preflight ok: {preflight}")
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
        market_source=str(args.market_source),
        collector_config_path=str(args.collector_config),
        log_mode=str(args.log_mode),
        heartbeat_sec=float(args.heartbeat_sec),
        show_orderbook=_parse_bool(args.show_orderbook, default=False),
        terminal_dashboard=_parse_bool(args.terminal_dashboard, default=False),
        dashboard_rows=int(args.dashboard_rows),
        api_timeout_sec=(float(args.api_timeout_sec) if float(args.api_timeout_sec) > 0 else None),
        api_retries=(int(args.api_retries) if int(args.api_retries) >= 0 else None),
        api_backoff_sec=(float(args.api_backoff_sec) if float(args.api_backoff_sec) >= 0 else None),
    )
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())

