from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.io.storage_writer import JsonlStorageWriter, iso_utc


def parse_utc(raw: Any) -> datetime | None:
    txt = str(raw or "").strip()
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


def floor_to_bucket(now_utc: datetime, bucket_min: int) -> datetime:
    minute = (now_utc.minute // max(1, int(bucket_min))) * int(bucket_min)
    return now_utc.replace(minute=minute, second=0, microsecond=0)


def parse_json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return parsed
    return []


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_levels(levels: Any, *, depth: int, reverse: bool) -> list[dict[str, float]]:
    if not isinstance(levels, list):
        return []
    out: list[dict[str, float]] = []
    for item in levels:
        if not isinstance(item, dict):
            continue
        price = safe_float(item.get("price"))
        size = safe_float(item.get("size"))
        if price is None or size is None:
            continue
        if not (0.0 <= float(price) <= 1.0) or float(size) < 0.0:
            continue
        out.append({"price": float(price), "size": float(size)})
    out.sort(key=lambda x: x["price"], reverse=reverse)
    if int(depth) > 0:
        out = out[: int(depth)]
    return out


def best_price(levels: list[dict[str, float]]) -> float | None:
    if not levels:
        return None
    return float(levels[0]["price"])


@dataclass(frozen=True)
class RequestConfig:
    timeout_sec: float = 8.0
    retries: int = 2
    backoff_sec: float = 0.25


@dataclass(frozen=True)
class MarketConfig:
    market_key_prefix: str = "SOL5M"
    coin: str = "sol"
    bucket_min: int = 5
    slug_static: str = ""
    slug_prefix: str = "sol-updown-5m-"
    labels_up: tuple[str, ...] = ("up", "yes")
    labels_down: tuple[str, ...] = ("down", "no")


@dataclass(frozen=True)
class EndpointConfig:
    gamma_host: str = "https://gamma-api.polymarket.com"
    clob_host: str = "https://clob.polymarket.com"
    data_api_host: str = "https://data-api.polymarket.com"


@dataclass(frozen=True)
class CollectorConfig:
    source: str
    market_folder: str
    market: MarketConfig
    endpoints: EndpointConfig
    request: RequestConfig


@dataclass
class ResolvedMarket:
    slug: str = ""
    condition_id: str = ""
    token_up: str = ""
    token_down: str = ""
    label_up: str = "up"
    label_down: str = "down"
    market_key: str = ""
    market_close_utc: str = ""


def load_collector_config(path: str | Path) -> CollectorConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("collector config must be a JSON object")
    market_raw = payload.get("market", {})
    if not isinstance(market_raw, dict):
        market_raw = {}
    endpoints_raw = payload.get("endpoints", {})
    if not isinstance(endpoints_raw, dict):
        endpoints_raw = {}
    request_raw = payload.get("request", {})
    if not isinstance(request_raw, dict):
        request_raw = {}

    labels_raw = market_raw.get("labels", {})
    if not isinstance(labels_raw, dict):
        labels_raw = {}

    market = MarketConfig(
        market_key_prefix=str(market_raw.get("market_key_prefix", "SOL5M")).strip().upper(),
        coin=str(market_raw.get("coin", "sol")).strip().lower(),
        bucket_min=max(1, int(market_raw.get("bucket_min", 5))),
        slug_static=str(market_raw.get("slug_static", "")).strip().lower(),
        slug_prefix=str(market_raw.get("slug_prefix", "sol-updown-5m-")).strip().lower(),
        labels_up=tuple(str(x).strip().lower() for x in labels_raw.get("up", ["up", "yes"])),
        labels_down=tuple(str(x).strip().lower() for x in labels_raw.get("down", ["down", "no"])),
    )
    endpoints = EndpointConfig(
        gamma_host=str(endpoints_raw.get("gamma_host", "https://gamma-api.polymarket.com")).rstrip("/"),
        clob_host=str(endpoints_raw.get("clob_host", "https://clob.polymarket.com")).rstrip("/"),
        data_api_host=str(endpoints_raw.get("data_api_host", "https://data-api.polymarket.com")).rstrip("/"),
    )
    request = RequestConfig(
        timeout_sec=float(request_raw.get("timeout_sec", 8.0)),
        retries=max(0, int(request_raw.get("retries", 2))),
        backoff_sec=max(0.0, float(request_raw.get("backoff_sec", 0.25))),
    )
    source = str(payload.get("source", "polymarket")).strip().lower()
    market_folder = str(payload.get("market_folder", "sol5m")).strip().lower() or "sol5m"
    return CollectorConfig(
        source=source,
        market_folder=market_folder,
        market=market,
        endpoints=endpoints,
        request=request,
    )


class MarketDataCollector:
    def __init__(
        self,
        *,
        config: CollectorConfig,
        raw_dir: str | Path,
        interval_sec: float = 1.0,
        runtime_sec: int = 0,
        book_depth: int = 15,
        trade_limit: int = 150,
        collect_orderbook: bool = True,
        collect_trades: bool = True,
        checkpoint_file: str | Path | None = None,
        last_closed_file: str | Path | None = None,
    ) -> None:
        self.config = config
        self.raw_dir = Path(raw_dir)
        self.interval_sec = max(0.1, float(interval_sec))
        self.runtime_sec = max(0, int(runtime_sec))
        self.book_depth = max(0, int(book_depth))
        self.trade_limit = max(1, int(trade_limit))
        self.collect_orderbook = bool(collect_orderbook)
        self.collect_trades = bool(collect_trades)
        self.checkpoint_file = Path(checkpoint_file) if checkpoint_file else None
        self.last_closed_file = Path(last_closed_file) if last_closed_file else None
        self.started_at_utc = iso_utc()
        self.storage = JsonlStorageWriter(
            raw_dir=self.raw_dir,
            market_folder=self.config.market_folder,
            source=self.config.source,
            started_at_utc=self.started_at_utc,
        )
        self.market = ResolvedMarket()
        self.seen_trade_ids: set[str] = set()
        self.discarded_in_cycle = 0
        self._load_checkpoint()

    def _write_last_closed_market(self, closed_market: ResolvedMarket, *, now_utc: datetime) -> None:
        if self.last_closed_file is None:
            return
        market_key = str(closed_market.market_key or "").strip()
        condition_id = str(closed_market.condition_id or "").strip()
        close_time_utc = str(closed_market.market_close_utc or "").strip()
        if not market_key or not condition_id or not close_time_utc:
            return
        payload = {
            "market_key": market_key,
            "condition_id": condition_id,
            "close_time_utc": close_time_utc,
            "market_slug": str(closed_market.slug or "").strip(),
            "generated_at_utc": iso_utc(now_utc),
        }
        self.last_closed_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.last_closed_file.with_suffix(self.last_closed_file.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp.replace(self.last_closed_file)

    def _build_slug(self, now_utc: datetime) -> str:
        if self.config.market.slug_static:
            return str(self.config.market.slug_static).lower()
        start_utc = floor_to_bucket(now_utc, self.config.market.bucket_min)
        return f"{self.config.market.slug_prefix}{int(start_utc.timestamp())}"

    def _market_close_from_slug(self, slug: str) -> str:
        try:
            start_ts = int(str(slug).rsplit("-", 1)[-1])
        except Exception:
            return ""
        dt = datetime.fromtimestamp(start_ts, tz=timezone.utc) + timedelta(minutes=self.config.market.bucket_min)
        return iso_utc(dt)

    def _market_key(self, close_utc: str) -> str:
        return f"{self.config.market.market_key_prefix}_{close_utc}" if close_utc else ""

    def _http_get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        query = urllib.parse.urlencode(params or {})
        full_url = f"{url}?{query}" if query else url
        last_error: Exception | None = None
        for attempt in range(self.config.request.retries + 1):
            try:
                req = urllib.request.Request(
                    full_url,
                    method="GET",
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                        "Accept": "application/json,text/plain,*/*",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Origin": "https://polymarket.com",
                        "Referer": "https://polymarket.com/",
                    },
                )
                with urllib.request.urlopen(req, timeout=float(self.config.request.timeout_sec)) as resp:
                    raw = resp.read()
                return json.loads(raw.decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt < self.config.request.retries:
                    backoff = float(self.config.request.backoff_sec) * (2**attempt)
                    time.sleep(backoff)
        raise RuntimeError(f"http_get_failed {full_url}: {last_error}")

    def _load_checkpoint(self) -> None:
        if not self.checkpoint_file or not self.checkpoint_file.exists():
            return
        try:
            payload = json.loads(self.checkpoint_file.read_text(encoding="utf-8"))
        except Exception:
            self.storage.add_warning("checkpoint_parse_error")
            return
        if not isinstance(payload, dict):
            self.storage.add_warning("checkpoint_invalid_format")
            return
        seen = payload.get("seen_trade_ids", [])
        if isinstance(seen, list):
            self.seen_trade_ids = {str(x) for x in seen if str(x).strip()}
        market = payload.get("market", {})
        if isinstance(market, dict):
            self.market = ResolvedMarket(
                slug=str(market.get("slug", "")),
                condition_id=str(market.get("condition_id", "")),
                token_up=str(market.get("token_up", "")),
                token_down=str(market.get("token_down", "")),
                label_up=str(market.get("label_up", "up")),
                label_down=str(market.get("label_down", "down")),
                market_key=str(market.get("market_key", "")),
                market_close_utc=str(market.get("market_close_utc", "")),
            )

    def _save_checkpoint(self) -> None:
        if not self.checkpoint_file:
            return
        payload = {
            "updated_at_utc": iso_utc(),
            "market": {
                "slug": self.market.slug,
                "condition_id": self.market.condition_id,
                "token_up": self.market.token_up,
                "token_down": self.market.token_down,
                "label_up": self.market.label_up,
                "label_down": self.market.label_down,
                "market_key": self.market.market_key,
                "market_close_utc": self.market.market_close_utc,
            },
            "seen_trade_ids": list(sorted(self.seen_trade_ids))[-5000:],
        }
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.checkpoint_file.with_suffix(self.checkpoint_file.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp.replace(self.checkpoint_file)

    def _resolve_market(self, *, slug: str) -> None:
        response = self._http_get_json(f"{self.config.endpoints.gamma_host}/events", params={"slug": slug})
        if not isinstance(response, list) or not response:
            raise RuntimeError(f"market_not_found slug={slug}")
        event = response[0] if isinstance(response[0], dict) else {}
        markets = event.get("markets", [])
        if not isinstance(markets, list) or not markets:
            raise RuntimeError(f"market_without_markets slug={slug}")

        selected: dict[str, Any] | None = None
        for item in markets:
            if not isinstance(item, dict):
                continue
            question = str(item.get("question", "")).lower()
            if "up or down" in question:
                selected = item
                break
        if selected is None:
            selected = markets[0] if isinstance(markets[0], dict) else None
        if selected is None:
            raise RuntimeError(f"market_selection_failed slug={slug}")

        outcomes = [str(x) for x in parse_json_list(selected.get("outcomes"))]
        token_ids = [str(x) for x in parse_json_list(selected.get("clobTokenIds"))]
        if len(outcomes) < 2 or len(token_ids) < 2:
            raise RuntimeError("market_tokens_missing")

        up_idx = next((i for i, out in enumerate(outcomes) if out.strip().lower() in self.config.market.labels_up), 0)
        down_idx = next(
            (i for i, out in enumerate(outcomes) if out.strip().lower() in self.config.market.labels_down and i != up_idx),
            1 if up_idx == 0 else 0,
        )

        close_utc = self._market_close_from_slug(slug)
        self.market = ResolvedMarket(
            slug=slug,
            condition_id=str(selected.get("conditionId", "")).strip(),
            token_up=token_ids[up_idx],
            token_down=token_ids[down_idx],
            label_up=outcomes[up_idx],
            label_down=outcomes[down_idx],
            market_key=self._market_key(close_utc),
            market_close_utc=close_utc,
        )
        if not self.market.market_key:
            raise RuntimeError("market_key_empty")

    def _fetch_book(self, token_id: str) -> dict[str, Any]:
        response = self._http_get_json(f"{self.config.endpoints.clob_host}/book", params={"token_id": token_id})
        if not isinstance(response, dict):
            raise RuntimeError("book_invalid_payload")
        return response

    def _fetch_midpoint(self, token_id: str) -> float | None:
        response = self._http_get_json(f"{self.config.endpoints.clob_host}/midpoint", params={"token_id": token_id})
        if not isinstance(response, dict):
            return None
        return safe_float(response.get("mid"), default=None)

    def _fetch_trades(self, condition_id: str) -> list[dict[str, Any]]:
        response = self._http_get_json(
            f"{self.config.endpoints.data_api_host}/trades",
            params={"market": condition_id, "limit": self.trade_limit, "offset": 0},
        )
        if not isinstance(response, list):
            return []
        return [row for row in response if isinstance(row, dict)]

    def _normalize_trade(self, raw: dict[str, Any], *, now_utc: datetime) -> dict[str, Any] | None:
        collector_ts_utc = iso_utc(now_utc)
        collector_cycle_id = str(int(now_utc.timestamp() * 1000))
        ts_raw = raw.get("timestamp")
        ts_dt: datetime | None = None
        if isinstance(ts_raw, (int, float)):
            ts_dt = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
        else:
            ts_dt = parse_utc(ts_raw)
        if ts_dt is None:
            self.storage.register_discard("trade_invalid_timestamp")
            return None

        price = safe_float(raw.get("price"))
        size = safe_float(raw.get("size"))
        side = str(raw.get("side", "")).strip().lower()
        if price is None or size is None or side == "":
            self.storage.register_discard("trade_missing_fields")
            return None
        if not (0.0 <= float(price) <= 1.0):
            self.storage.register_discard("trade_price_out_of_range")
            return None
        if float(size) < 0.0:
            self.storage.register_discard("trade_negative_size")
            return None

        asset = str(raw.get("asset", "")).strip()
        tx = str(raw.get("transactionHash", "")).strip()
        tid = str(raw.get("id", "")).strip()
        trade_id = tid or f"{tx}:{asset}:{raw.get('timestamp', '')}"
        if not trade_id.strip():
            self.storage.register_discard("trade_missing_trade_id")
            return None
        outcome = ""
        if asset == self.market.token_up:
            outcome = "up"
        elif asset == self.market.token_down:
            outcome = "down"

        return {
            "timestamp_utc": iso_utc(ts_dt),
            "collector_ts_utc": collector_ts_utc,
            "collector_cycle_id": collector_cycle_id,
            "market_key": self.market.market_key,
            "condition_id": self.market.condition_id,
            "market_slug": self.market.slug,
            "market_close_utc": self.market.market_close_utc,
            "trade_id": trade_id,
            "side": side,
            "price": float(price),
            "size": float(size),
            "outcome": outcome,
            "asset": asset,
            "transaction_hash": tx,
            "token_up": self.market.token_up,
            "token_down": self.market.token_down,
            "source": self.config.source,
        }

    def collect_once(self, *, now_utc: datetime | None = None) -> dict[str, int]:
        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        collector_ts_utc = iso_utc(now)
        collector_cycle_id = str(int(now.timestamp() * 1000))
        self.storage.ensure_daily_files(now_utc=now)
        counts = {"prices": 0, "orderbook": 0, "trades": 0}

        slug = self._build_slug(now)
        previous_market: ResolvedMarket | None = None
        if slug != self.market.slug and self.market.market_key:
            previous_market = ResolvedMarket(
                slug=self.market.slug,
                condition_id=self.market.condition_id,
                token_up=self.market.token_up,
                token_down=self.market.token_down,
                label_up=self.market.label_up,
                label_down=self.market.label_down,
                market_key=self.market.market_key,
                market_close_utc=self.market.market_close_utc,
            )
        if slug != self.market.slug or not self.market.market_key:
            try:
                self._resolve_market(slug=slug)
                if previous_market is not None:
                    self._write_last_closed_market(previous_market, now_utc=now)
            except Exception as exc:  # noqa: BLE001
                self.storage.add_error()
                self.storage.add_warning(f"market_resolve_error:{exc}")
                self.storage.write_metadata(now_utc=now)
                self._save_checkpoint()
                return counts

        if not self.market.market_key:
            self.storage.add_error()
            self.storage.add_warning("market_key_missing")
            self.storage.write_metadata(now_utc=now)
            self._save_checkpoint()
            return counts

        try:
            up_book = self._fetch_book(self.market.token_up)
            down_book = self._fetch_book(self.market.token_down)
            up_mid = self._fetch_midpoint(self.market.token_up)
        except Exception as exc:  # noqa: BLE001
            self.storage.add_error()
            self.storage.add_warning(f"price_or_book_fetch_error:{exc}")
            self.storage.write_metadata(now_utc=now)
            self._save_checkpoint()
            return counts

        up_bids = normalize_levels(up_book.get("bids"), depth=self.book_depth, reverse=True)
        up_asks = normalize_levels(up_book.get("asks"), depth=self.book_depth, reverse=False)
        down_bids = normalize_levels(down_book.get("bids"), depth=self.book_depth, reverse=True)
        down_asks = normalize_levels(down_book.get("asks"), depth=self.book_depth, reverse=False)

        best_bid = best_price(up_bids)
        best_ask = best_price(up_asks)
        midpoint = up_mid
        if midpoint is None and best_bid is not None and best_ask is not None:
            midpoint = (float(best_bid) + float(best_ask)) / 2.0
        spread = None
        if best_bid is not None and best_ask is not None:
            spread = float(best_ask) - float(best_bid)

        if best_bid is None or best_ask is None or midpoint is None or spread is None:
            self.storage.register_discard("prices_incomplete_book")
        else:
            self.storage.append_row(
                kind="prices",
                now_utc=now,
                row={
                    "timestamp_utc": collector_ts_utc,
                    "collector_ts_utc": collector_ts_utc,
                    "collector_cycle_id": collector_cycle_id,
                    "market_key": self.market.market_key,
                    "condition_id": self.market.condition_id,
                    "best_bid": float(best_bid),
                    "best_ask": float(best_ask),
                    "midpoint": float(midpoint),
                    "spread": float(spread),
                    "source": self.config.source,
                    "market_slug": self.market.slug,
                    "market_close_utc": self.market.market_close_utc,
                    "token_up": self.market.token_up,
                    "token_down": self.market.token_down,
                },
            )
            counts["prices"] += 1

        if self.collect_orderbook:
            self.storage.append_row(
                kind="orderbook",
                now_utc=now,
                row={
                    "timestamp_utc": collector_ts_utc,
                    "collector_ts_utc": collector_ts_utc,
                    "collector_cycle_id": collector_cycle_id,
                    "market_key": self.market.market_key,
                    "condition_id": self.market.condition_id,
                    "bids": up_bids,
                    "asks": up_asks,
                    "depth_used": int(self.book_depth),
                    "source": self.config.source,
                    "market_slug": self.market.slug,
                    "market_close_utc": self.market.market_close_utc,
                    "token_up": self.market.token_up,
                    "token_down": self.market.token_down,
                    "yes_bids": up_bids,
                    "yes_asks": up_asks,
                    "no_bids": down_bids,
                    "no_asks": down_asks,
                },
            )
            counts["orderbook"] += 1

        if self.collect_trades and self.market.condition_id:
            try:
                raw_trades = self._fetch_trades(self.market.condition_id)
            except Exception as exc:  # noqa: BLE001
                self.storage.add_error()
                self.storage.add_warning(f"trades_fetch_error:{exc}")
                raw_trades = []
            for raw_trade in reversed(raw_trades):
                normalized = self._normalize_trade(raw_trade, now_utc=now)
                if normalized is None:
                    continue
                tid = str(normalized["trade_id"])
                if tid in self.seen_trade_ids:
                    continue
                self.seen_trade_ids.add(tid)
                self.storage.append_row(kind="trades", now_utc=now, row=normalized)
                counts["trades"] += 1
            if len(self.seen_trade_ids) > 10000:
                self.seen_trade_ids = set(sorted(self.seen_trade_ids)[-5000:])

        self.storage.write_metadata(now_utc=now)
        self._save_checkpoint()
        return counts

    def run(self) -> int:
        started = time.time()
        print(
            f"[collect-sol5m] source={self.config.source} interval_sec={self.interval_sec:.2f} "
            f"runtime_sec={self.runtime_sec} depth={self.book_depth} trade_limit={self.trade_limit} "
            f"orderbook={self.collect_orderbook} trades={self.collect_trades}"
        )
        try:
            while True:
                cycle_start = time.time()
                counts = self.collect_once()
                print(
                    f"{iso_utc()} | market={self.market.market_key or '-'} "
                    f"rows(prices={counts['prices']},orderbook={counts['orderbook']},trades={counts['trades']})"
                )
                if self.runtime_sec > 0 and (time.time() - started) >= self.runtime_sec:
                    break
                elapsed = time.time() - cycle_start
                sleep_for = max(0.0, self.interval_sec - elapsed)
                if sleep_for > 0:
                    time.sleep(sleep_for)
        except KeyboardInterrupt:
            print("[collect-sol5m] interrupted by Ctrl+C")
        finally:
            self.storage.write_metadata(now_utc=datetime.now(timezone.utc))
            self._save_checkpoint()
        return 0
