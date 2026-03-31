from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


GROUP_RULES = (
    ("raw", Path("data/raw"), "keep_days_raw"),
    ("reports_live", Path("reports/live"), "keep_days_reports"),
    ("reports_tuning_runs", Path("reports/tuning/_runs"), "keep_days_reports"),
    ("logs", Path("logs"), "keep_days_logs"),
)


@dataclass(frozen=True)
class RetentionAction:
    group: str
    path: Path
    age_days: float
    reason: str


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid bool value: {value}")


def parse_now_utc(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_retention_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return {
        "keep_days_raw": int(data.get("keep_days_raw", 3)),
        "keep_days_reports": int(data.get("keep_days_reports", 7)),
        "keep_days_logs": int(data.get("keep_days_logs", 7)),
        "max_keep_files_per_group": int(data.get("max_keep_files_per_group", 50)),
        "dry_run_default": bool(data.get("dry_run_default", True)),
        "archive_dir": str(data.get("archive_dir", "archives/runtime")),
    }


def list_group_files(base_dir: Path, rel_dir: Path) -> list[Path]:
    root = base_dir / rel_dir
    if not root.exists():
        return []
    return sorted((p for p in root.rglob("*") if p.is_file()), key=lambda item: item.stat().st_mtime, reverse=True)


def build_retention_plan(
    *,
    base_dir: Path,
    policy: dict[str, Any],
    now_utc: datetime,
) -> tuple[list[RetentionAction], int]:
    actions: list[RetentionAction] = []
    scanned_files = 0
    max_keep = max(0, int(policy["max_keep_files_per_group"]))
    now_ts = now_utc.timestamp()

    for group_name, rel_dir, keep_key in GROUP_RULES:
        keep_days = int(policy[keep_key])
        files = list_group_files(base_dir, rel_dir)
        scanned_files += len(files)
        for index, file_path in enumerate(files):
            age_days = max(0.0, (now_ts - file_path.stat().st_mtime) / 86400.0)
            reason: str | None = None
            if index >= max_keep:
                reason = "over_max_keep_files"
            if age_days > keep_days:
                reason = "older_than_keep_days" if reason is None else f"{reason}+older_than_keep_days"
            if reason is not None:
                actions.append(
                    RetentionAction(
                        group=group_name,
                        path=file_path,
                        age_days=age_days,
                        reason=reason,
                    )
                )
    return actions, scanned_files


def _archive_destination(base_dir: Path, archive_dir: Path, action: RetentionAction, now_utc: datetime) -> Path:
    rel_path = action.path.relative_to(base_dir)
    date_stamp = now_utc.strftime("%Y-%m-%d")
    return archive_dir / "cleanup" / date_stamp / rel_path


def execute_retention_plan(
    *,
    base_dir: Path,
    actions: list[RetentionAction],
    scanned_files: int,
    archive: bool,
    dry_run: bool,
    now_utc: datetime,
    archive_dir: Path,
) -> dict[str, Any]:
    kept_files = max(0, scanned_files - len(actions))
    summary: dict[str, Any] = {
        "scanned_files": scanned_files,
        "kept_files": kept_files,
        "archived_files": 0,
        "deleted_files": 0,
        "would_archive_files": 0,
        "would_delete_files": 0,
        "dry_run": dry_run,
        "archive_mode": archive,
    }

    for action in actions:
        if archive:
            if dry_run:
                summary["would_archive_files"] += 1
                continue
            destination = _archive_destination(base_dir, archive_dir, action, now_utc)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(action.path), str(destination))
            summary["archived_files"] += 1
        else:
            if dry_run:
                summary["would_delete_files"] += 1
                continue
            action.path.unlink(missing_ok=True)
            summary["deleted_files"] += 1

    return summary


def run_cleanup(
    *,
    base_dir: Path,
    config_path: Path,
    dry_run: bool | None = None,
    archive: bool = False,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    policy = load_retention_config(config_path)
    effective_now = now_utc or datetime.now(timezone.utc)
    effective_dry_run = policy["dry_run_default"] if dry_run is None else bool(dry_run)
    actions, scanned_files = build_retention_plan(base_dir=base_dir, policy=policy, now_utc=effective_now)
    archive_dir = base_dir / str(policy["archive_dir"])
    summary = execute_retention_plan(
        base_dir=base_dir,
        actions=actions,
        scanned_files=scanned_files,
        archive=archive,
        dry_run=bool(effective_dry_run),
        now_utc=effective_now,
        archive_dir=archive_dir,
    )
    summary["policy"] = policy
    summary["candidate_files"] = len(actions)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cleanup runtime artifacts based on retention policy.")
    parser.add_argument("--dry-run", default=None, help="true|false. If omitted, uses policy dry_run_default.")
    parser.add_argument("--archive", default="false", help="true|false. Archive instead of deleting files.")
    parser.add_argument("--config", default="configs/artifact_retention.json")
    parser.add_argument("--now-utc", default=None, help="Optional ISO timestamp in UTC.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    dry_run_value = None if args.dry_run is None else parse_bool(args.dry_run)
    archive_value = parse_bool(args.archive)
    now_utc = parse_now_utc(args.now_utc)
    summary = run_cleanup(
        base_dir=PROJECT_ROOT,
        config_path=PROJECT_ROOT / args.config,
        dry_run=dry_run_value,
        archive=archive_value,
        now_utc=now_utc,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
