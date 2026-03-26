from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def normalize_trade_id(row: dict[str, Any]) -> str:
    direct = str(row.get("trade_id", "")).strip() or str(row.get("id", "")).strip()
    if direct:
        return direct
    tx_hash = str(row.get("transactionHash", "") or row.get("transaction_hash", "")).strip()
    asset = str(row.get("asset", "") or row.get("asset_id", "")).strip()
    ts = row.get("timestamp", row.get("match_time", row.get("timestamp_utc", "")))
    if tx_hash and asset and str(ts).strip():
        return f"{tx_hash}:{asset}:{ts}"
    return ""


def dedupe_trade_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        trade_id = normalize_trade_id(row)
        if not trade_id:
            continue
        if trade_id in seen:
            continue
        seen.add(trade_id)
        payload = dict(row)
        payload["trade_id"] = trade_id
        deduped.append(payload)
    return deduped


def reconcile_trade_sets(
    *,
    realtime_rows: list[dict[str, Any]],
    backfill_rows: list[dict[str, Any]],
    market_key: str = "",
) -> dict[str, Any]:
    target_market_key = str(market_key).strip()

    def _filter(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not target_market_key:
            return rows
        return [r for r in rows if str(r.get("market_key", "")).strip() == target_market_key]

    realtime_unique = dedupe_trade_rows(_filter(realtime_rows))
    backfill_unique = dedupe_trade_rows(_filter(backfill_rows))

    realtime_ids = {str(row["trade_id"]) for row in realtime_unique}
    backfill_ids = {str(row["trade_id"]) for row in backfill_unique}

    overlap = sorted(realtime_ids.intersection(backfill_ids))
    missing = sorted(backfill_ids.difference(realtime_ids))
    extra = sorted(realtime_ids.difference(backfill_ids))

    backfill_count = len(backfill_ids)
    overlap_count = len(overlap)
    coverage_pct = (float(overlap_count) / float(backfill_count) * 100.0) if backfill_count > 0 else 0.0

    return {
        "market_key": target_market_key,
        "realtime_count": len(realtime_ids),
        "backfill_count": backfill_count,
        "overlap_count": overlap_count,
        "missing_in_realtime": missing,
        "missing_in_realtime_count": len(missing),
        "extra_in_realtime_count": len(extra),
        "coverage_pct": round(coverage_pct, 4),
    }


def reconcile_trade_files(
    *,
    realtime_file: str | Path,
    backfill_file: str | Path,
    market_key: str = "",
) -> dict[str, Any]:
    realtime_rows = load_jsonl(realtime_file)
    backfill_rows = load_jsonl(backfill_file)
    return reconcile_trade_sets(realtime_rows=realtime_rows, backfill_rows=backfill_rows, market_key=market_key)

