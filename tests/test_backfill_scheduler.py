from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.io.backfill_scheduler import BackfillScheduler, parse_utc


def _write_last_closed(path: Path, *, market_key: str, condition_id: str, close_time_utc: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "market_key": market_key,
        "condition_id": condition_id,
        "close_time_utc": close_time_utc,
    }
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def test_scheduler_detects_pending_closed_market(tmp_path: Path) -> None:
    last_closed = tmp_path / "reports" / "live" / "last_closed_market.json"
    state_file = tmp_path / "reports" / "live" / "backfill_state.json"
    close_dt = datetime(2026, 3, 26, 3, 0, 0, tzinfo=timezone.utc)
    _write_last_closed(
        last_closed,
        market_key="SOL5M_2026-03-26T03:00:00Z",
        condition_id="0xabc123",
        close_time_utc=close_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    scheduler = BackfillScheduler(last_closed_file=last_closed, state_file=state_file, backfill_interval_sec=300)
    pending = scheduler.get_pending_market(now_utc=close_dt + timedelta(seconds=301))
    assert pending is not None
    assert pending.market_key == "SOL5M_2026-03-26T03:00:00Z"
    assert pending.condition_id == "0xabc123"


def test_scheduler_does_not_reprocess_processed_market(tmp_path: Path) -> None:
    last_closed = tmp_path / "reports" / "live" / "last_closed_market.json"
    state_file = tmp_path / "reports" / "live" / "backfill_state.json"
    close_dt = datetime(2026, 3, 26, 3, 5, 0, tzinfo=timezone.utc)
    _write_last_closed(
        last_closed,
        market_key="SOL5M_2026-03-26T03:05:00Z",
        condition_id="0xdef456",
        close_time_utc=close_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    scheduler = BackfillScheduler(last_closed_file=last_closed, state_file=state_file, backfill_interval_sec=0)
    pending = scheduler.get_pending_market(now_utc=close_dt + timedelta(seconds=1))
    assert pending is not None

    scheduler.mark_processed(
        pending,
        backfill_meta_file="data/raw/sol5m/backfill/meta_0xdef456.json",
        reconciliation_file="reports/live/reconciliation_0xdef456.json",
        rows_backfill=10,
        rows_realtime=8,
    )

    scheduler_reloaded = BackfillScheduler(last_closed_file=last_closed, state_file=state_file, backfill_interval_sec=0)
    pending_after = scheduler_reloaded.get_pending_market(now_utc=close_dt + timedelta(seconds=2))
    assert pending_after is None


def test_scheduler_allows_refresh_attempts_by_interval(tmp_path: Path) -> None:
    last_closed = tmp_path / "reports" / "live" / "last_closed_market.json"
    state_file = tmp_path / "reports" / "live" / "backfill_state.json"
    close_dt = datetime(2026, 3, 26, 4, 0, 0, tzinfo=timezone.utc)
    _write_last_closed(
        last_closed,
        market_key="SOL5M_2026-03-26T04:00:00Z",
        condition_id="0xrefresh01",
        close_time_utc=close_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    scheduler = BackfillScheduler(last_closed_file=last_closed, state_file=state_file, backfill_interval_sec=0)

    first = scheduler.get_refresh_candidate(
        max_attempts=3,
        refresh_interval_sec=60,
        now_utc=close_dt + timedelta(seconds=1),
    )
    assert first is not None
    scheduler.mark_processed(first, max_attempts=3)
    first_attempt_at = parse_utc(
        scheduler.state["processed"]["0xrefresh01"]["last_attempt_at_utc"]
    )
    assert first_attempt_at is not None

    early_retry = scheduler.get_refresh_candidate(
        max_attempts=3,
        refresh_interval_sec=60,
        now_utc=first_attempt_at + timedelta(seconds=30),
    )
    assert early_retry is None

    second = scheduler.get_refresh_candidate(
        max_attempts=3,
        refresh_interval_sec=60,
        now_utc=first_attempt_at + timedelta(seconds=65),
    )
    assert second is not None
    scheduler.mark_processed(second, max_attempts=3)
    second_attempt_at = parse_utc(
        scheduler.state["processed"]["0xrefresh01"]["last_attempt_at_utc"]
    )
    assert second_attempt_at is not None

    third = scheduler.get_refresh_candidate(
        max_attempts=3,
        refresh_interval_sec=60,
        now_utc=second_attempt_at + timedelta(seconds=65),
    )
    assert third is not None
    scheduler.mark_processed(third, max_attempts=3)
    third_attempt_at = parse_utc(
        scheduler.state["processed"]["0xrefresh01"]["last_attempt_at_utc"]
    )
    assert third_attempt_at is not None

    capped = scheduler.get_refresh_candidate(
        max_attempts=3,
        refresh_interval_sec=60,
        now_utc=third_attempt_at + timedelta(seconds=65),
    )
    assert capped is None
