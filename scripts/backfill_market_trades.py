from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.io.market_data_collector import CollectorConfig, load_collector_config
from src.io.storage_writer import iso_utc


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _sanitize_filename_part(value: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", str(value or "").strip())
    return safe or "unknown"


def _parse_timestamp_utc(raw: Any) -> str:
    if isinstance(raw, (int, float)):
        return iso_utc(datetime.fromtimestamp(float(raw), tz=timezone.utc))
    text = str(raw or "").strip()
    if not text:
        return ""
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return iso_utc(dt.astimezone(timezone.utc))


def _build_trade_id(row: dict[str, Any]) -> str:
    direct = str(row.get("trade_id", "")).strip() or str(row.get("id", "")).strip()
    if direct:
        return direct
    tx_hash = str(row.get("transactionHash", "") or row.get("transaction_hash", "")).strip()
    asset = str(row.get("asset", "") or row.get("asset_id", "")).strip()
    timestamp = row.get("timestamp", row.get("match_time", row.get("timestamp_utc", "")))
    if tx_hash and asset and str(timestamp).strip():
        return f"{tx_hash}:{asset}:{timestamp}"
    return ""


def _http_get_json(
    *,
    url: str,
    params: dict[str, Any],
    timeout_sec: float,
    retries: int,
    backoff_sec: float,
) -> Any:
    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}" if query else url
    last_error: Exception | None = None
    for attempt in range(max(0, int(retries)) + 1):
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
            with urllib.request.urlopen(req, timeout=float(timeout_sec)) as resp:
                raw = resp.read()
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < int(retries):
                time.sleep(float(backoff_sec) * (2**attempt))
    raise RuntimeError(f"http_get_failed {full_url}: {last_error}")


def _normalize_backfill_trade(
    *,
    row: dict[str, Any],
    condition_id: str,
    market_key: str,
    source: str,
) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    trade_id = _build_trade_id(row)
    if not trade_id:
        return None
    timestamp_utc = _parse_timestamp_utc(row.get("timestamp", row.get("match_time", row.get("timestamp_utc"))))
    if not timestamp_utc:
        return None
    price = _safe_float(row.get("price"), default=None)
    size = _safe_float(row.get("size"), default=None)
    if price is None or size is None:
        return None
    return {
        "timestamp_utc": timestamp_utc,
        "market_key": str(market_key or "").strip(),
        "condition_id": str(condition_id).strip(),
        "trade_id": trade_id,
        "side": str(row.get("side", "")).strip().lower(),
        "price": float(price),
        "size": float(size),
        "outcome": str(row.get("outcome", "")).strip().lower(),
        "asset": str(row.get("asset", "") or row.get("asset_id", "")).strip(),
        "transaction_hash": str(row.get("transactionHash", "") or row.get("transaction_hash", "")).strip(),
        "source": str(source),
    }


def backfill_market_trades(
    *,
    condition_id: str,
    market_key: str,
    close_time_utc: str,
    config_path: str | Path = "configs/data_collection_sol5m.json",
    raw_dir: str | Path = "data/raw",
    page_size: int = 500,
    max_pages: int = 0,
) -> dict[str, Any]:
    cond = str(condition_id).strip()
    if not cond:
        raise ValueError("condition_id is required")

    cfg: CollectorConfig = load_collector_config(config_path)
    base_folder = Path(raw_dir) / cfg.market_folder / "backfill"
    base_folder.mkdir(parents=True, exist_ok=True)

    safe_cond = _sanitize_filename_part(cond)
    trades_path = base_folder / f"trades_{safe_cond}.jsonl"
    meta_path = base_folder / f"meta_{safe_cond}.json"

    started_at_utc = iso_utc()
    warnings: list[str] = []
    errors_count = 0
    pages_fetched = 0
    rows_raw_seen = 0
    rows_written = 0

    seen_trade_ids: set[str] = set()
    normalized_rows: list[dict[str, Any]] = []
    next_cursor: str | None = None
    offset = 0
    stale_pages = 0
    hard_page_cap = 1000

    while True:
        if max_pages > 0 and pages_fetched >= int(max_pages):
            break
        if pages_fetched >= hard_page_cap:
            warnings.append("hard_page_cap_reached")
            break

        params: dict[str, Any] = {"market": cond, "limit": int(page_size)}
        cursor_mode = next_cursor is not None
        if cursor_mode:
            params["next_cursor"] = str(next_cursor)
        else:
            params["offset"] = int(offset)

        try:
            payload = _http_get_json(
                url=f"{cfg.endpoints.data_api_host}/trades",
                params=params,
                timeout_sec=float(cfg.request.timeout_sec),
                retries=int(cfg.request.retries),
                backoff_sec=float(cfg.request.backoff_sec),
            )
        except Exception as exc:  # noqa: BLE001
            errors_count += 1
            warnings.append(f"backfill_fetch_error:{exc}")
            break

        pages_fetched += 1
        page_rows: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            data = payload.get("data", [])
            if isinstance(data, list):
                page_rows = [row for row in data if isinstance(row, dict)]
            cursor = str(payload.get("next_cursor", "")).strip()
            if cursor and cursor not in {"-1", "LTE="}:
                next_cursor = cursor
            else:
                next_cursor = None
        elif isinstance(payload, list):
            page_rows = [row for row in payload if isinstance(row, dict)]
            next_cursor = None
        else:
            page_rows = []

        rows_raw_seen += len(page_rows)
        if not page_rows:
            break

        added_this_page = 0
        for raw in page_rows:
            normalized = _normalize_backfill_trade(
                row=raw,
                condition_id=cond,
                market_key=market_key,
                source=cfg.source,
            )
            if normalized is None:
                continue
            trade_id = str(normalized["trade_id"])
            if trade_id in seen_trade_ids:
                continue
            seen_trade_ids.add(trade_id)
            normalized_rows.append(normalized)
            rows_written += 1
            added_this_page += 1

        if added_this_page == 0:
            stale_pages += 1
        else:
            stale_pages = 0

        if stale_pages >= 2:
            warnings.append("stale_pagination_detected")
            break

        if next_cursor is not None:
            continue

        offset += int(page_size)

    normalized_rows.sort(key=lambda row: str(row.get("timestamp_utc", "")))
    with trades_path.open("w", encoding="utf-8", newline="\n") as fh:
        for row in normalized_rows:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")

    finished_at_utc = iso_utc()
    meta_payload = {
        "market_key": str(market_key or "").strip(),
        "condition_id": cond,
        "close_time_utc": str(close_time_utc or "").strip(),
        "source": cfg.source,
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc,
        "pages_fetched": int(pages_fetched),
        "rows_raw_seen": int(rows_raw_seen),
        "rows_written": int(rows_written),
        "deduped_rows": int(max(0, rows_raw_seen - rows_written)),
        "errors_count": int(errors_count),
        "warnings": warnings,
        "page_size": int(page_size),
        "max_pages": int(max_pages),
        "output_file": str(trades_path),
    }
    meta_path.write_text(json.dumps(meta_payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return {
        "trades_file": str(trades_path),
        "meta_file": str(meta_path),
        **meta_payload,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill all trades for one SOL5M closed market by condition_id.")
    parser.add_argument("--condition-id", required=True, help="Condition ID of the closed market.")
    parser.add_argument("--market-key", default="", help="Optional market key used for reconciliation.")
    parser.add_argument("--close-time-utc", default="", help="Optional close timestamp (UTC ISO-8601).")
    parser.add_argument("--config", default="configs/data_collection_sol5m.json")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=0, help="0 = fetch until pagination end.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = backfill_market_trades(
        condition_id=str(args.condition_id),
        market_key=str(args.market_key),
        close_time_utc=str(args.close_time_utc),
        config_path=str(args.config),
        raw_dir=str(args.raw_dir),
        page_size=max(1, int(args.page_size)),
        max_pages=max(0, int(args.max_pages)),
    )
    print(
        f"[backfill] condition_id={result['condition_id']} "
        f"pages={result['pages_fetched']} rows={result['rows_written']} "
        f"trades_file={result['trades_file']}"
    )
    print(f"[backfill] meta_file={result['meta_file']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

