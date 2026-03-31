from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.cleanup_artifacts import build_retention_plan, load_retention_config, run_cleanup


def _write_file(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _set_mtime_utc(path: Path, dt: datetime) -> None:
    ts = dt.timestamp()
    os.utime(path, (ts, ts))


def _write_config(base_dir: Path, cfg: dict[str, object]) -> Path:
    config_path = base_dir / "configs" / "artifact_retention.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(cfg, ensure_ascii=True), encoding="utf-8")
    return config_path


def _policy() -> dict[str, object]:
    return {
        "keep_days_raw": 3,
        "keep_days_reports": 7,
        "keep_days_logs": 7,
        "max_keep_files_per_group": 50,
        "dry_run_default": True,
        "archive_dir": "archives/runtime",
    }


def test_selection_by_age_marks_only_old_files(tmp_path: Path) -> None:
    now = datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc)
    cfg_path = _write_config(tmp_path, _policy())
    policy = load_retention_config(cfg_path)

    fresh = tmp_path / "data" / "raw" / "fresh.jsonl"
    old = tmp_path / "data" / "raw" / "old.jsonl"
    _write_file(fresh)
    _write_file(old)
    _set_mtime_utc(fresh, now - timedelta(days=1))
    _set_mtime_utc(old, now - timedelta(days=5))

    actions, scanned = build_retention_plan(base_dir=tmp_path, policy=policy, now_utc=now)
    paths = {a.path for a in actions}

    assert scanned == 2
    assert old in paths
    assert fresh not in paths


def test_dry_run_does_not_delete_files(tmp_path: Path) -> None:
    now = datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc)
    cfg_path = _write_config(tmp_path, _policy())
    old_log = tmp_path / "logs" / "bot.log"
    _write_file(old_log)
    _set_mtime_utc(old_log, now - timedelta(days=10))

    summary = run_cleanup(
        base_dir=tmp_path,
        config_path=cfg_path,
        dry_run=True,
        archive=False,
        now_utc=now,
    )

    assert old_log.exists()
    assert int(summary["deleted_files"]) == 0
    assert int(summary["would_delete_files"]) == 1


def test_delete_and_archive_modes_apply_policy(tmp_path: Path) -> None:
    now = datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc)
    cfg_path = _write_config(tmp_path, _policy())

    delete_target = tmp_path / "reports" / "live" / "old.csv"
    _write_file(delete_target)
    _set_mtime_utc(delete_target, now - timedelta(days=15))
    delete_summary = run_cleanup(
        base_dir=tmp_path,
        config_path=cfg_path,
        dry_run=False,
        archive=False,
        now_utc=now,
    )
    assert not delete_target.exists()
    assert int(delete_summary["deleted_files"]) == 1

    archive_target = tmp_path / "data" / "raw" / "old2.jsonl"
    _write_file(archive_target)
    _set_mtime_utc(archive_target, now - timedelta(days=15))
    archive_summary = run_cleanup(
        base_dir=tmp_path,
        config_path=cfg_path,
        dry_run=False,
        archive=True,
        now_utc=now,
    )
    archived_copy = tmp_path / "archives" / "runtime" / "cleanup" / "2026-03-31" / "data" / "raw" / "old2.jsonl"
    assert not archive_target.exists()
    assert archived_copy.exists()
    assert int(archive_summary["archived_files"]) == 1


def test_summary_counts_are_correct(tmp_path: Path) -> None:
    now = datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _policy()
    cfg["max_keep_files_per_group"] = 1
    cfg_path = _write_config(tmp_path, cfg)

    newest = tmp_path / "logs" / "new.log"
    older = tmp_path / "logs" / "old.log"
    _write_file(newest)
    _write_file(older)
    _set_mtime_utc(newest, now - timedelta(days=1))
    _set_mtime_utc(older, now - timedelta(days=2))

    summary = run_cleanup(
        base_dir=tmp_path,
        config_path=cfg_path,
        dry_run=True,
        archive=False,
        now_utc=now,
    )

    assert int(summary["scanned_files"]) == 2
    assert int(summary["kept_files"]) == 1
    assert int(summary["candidate_files"]) == 1
    assert int(summary["would_delete_files"]) == 1
