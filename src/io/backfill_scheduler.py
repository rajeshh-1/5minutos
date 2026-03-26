from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class ClosedMarket:
    market_key: str
    condition_id: str
    close_time_utc: str


class BackfillScheduler:
    def __init__(
        self,
        *,
        last_closed_file: str | Path,
        state_file: str | Path,
        backfill_interval_sec: int = 300,
    ) -> None:
        self.last_closed_file = Path(last_closed_file)
        self.state_file = Path(state_file)
        self.backfill_interval_sec = max(0, int(backfill_interval_sec))
        self.state: dict[str, Any] = self._load_state()

    @staticmethod
    def _market_key(market: ClosedMarket) -> str:
        condition_id = str(market.condition_id).strip()
        if condition_id:
            return condition_id
        return str(market.market_key).strip()

    def _load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return {"processed": {}}
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except Exception:
            return {"processed": {}}
        if not isinstance(payload, dict):
            return {"processed": {}}
        processed = payload.get("processed")
        if not isinstance(processed, dict):
            processed = {}
        return {"processed": processed}

    def _save_state(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(self.state)
        payload["updated_at_utc"] = iso_utc_now()
        tmp = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp.replace(self.state_file)

    def load_last_closed_market(self) -> ClosedMarket | None:
        if not self.last_closed_file.exists():
            return None
        try:
            payload = json.loads(self.last_closed_file.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        market_key = str(payload.get("market_key", "")).strip()
        condition_id = str(payload.get("condition_id", "")).strip()
        close_time_utc = str(payload.get("close_time_utc", "")).strip()
        if not market_key or not condition_id or not close_time_utc:
            return None
        if parse_utc(close_time_utc) is None:
            return None
        return ClosedMarket(market_key=market_key, condition_id=condition_id, close_time_utc=close_time_utc)

    def is_processed(self, market: ClosedMarket) -> bool:
        return self._market_key(market) in self.state.get("processed", {})

    def is_eligible_by_age(self, market: ClosedMarket, *, now_utc: datetime | None = None) -> bool:
        close_dt = parse_utc(market.close_time_utc)
        if close_dt is None:
            return False
        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        due_dt = close_dt + timedelta(seconds=int(self.backfill_interval_sec))
        return now >= due_dt

    def get_pending_market(self, *, now_utc: datetime | None = None) -> ClosedMarket | None:
        market = self.load_last_closed_market()
        if market is None:
            return None
        if self.is_processed(market):
            return None
        if not self.is_eligible_by_age(market, now_utc=now_utc):
            return None
        return market

    def mark_processed(
        self,
        market: ClosedMarket,
        *,
        backfill_meta_file: str = "",
        reconciliation_file: str = "",
        rows_backfill: int = 0,
        rows_realtime: int = 0,
    ) -> None:
        key = self._market_key(market)
        processed = self.state.setdefault("processed", {})
        processed[key] = {
            "market_key": market.market_key,
            "condition_id": market.condition_id,
            "close_time_utc": market.close_time_utc,
            "processed_at_utc": iso_utc_now(),
            "backfill_meta_file": str(backfill_meta_file or ""),
            "reconciliation_file": str(reconciliation_file or ""),
            "rows_backfill": int(rows_backfill),
            "rows_realtime": int(rows_realtime),
        }
        self._save_state()

