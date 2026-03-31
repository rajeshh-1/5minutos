from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.cleanup_artifacts import (
    build_retention_plan,
    load_retention_config,
    parse_bool,
    parse_now_utc,
)


def _zip_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _load_index(index_path: Path) -> dict[str, object]:
    if not index_path.exists():
        return {"generated_at_utc": "", "archives": []}
    with index_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        return {"generated_at_utc": "", "archives": []}
    archives = data.get("archives")
    if not isinstance(archives, list):
        data["archives"] = []
    return data


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive old runtime artifacts into zip files.")
    parser.add_argument("--config", default="configs/artifact_retention.json")
    parser.add_argument("--dry-run", default="true", help="true|false")
    parser.add_argument("--delete-after-archive", default="false", help="true|false")
    parser.add_argument("--now-utc", default=None, help="Optional ISO timestamp in UTC.")
    return parser.parse_args(argv)


def run_archive(
    *,
    base_dir: Path,
    config_path: Path,
    dry_run: bool,
    delete_after_archive: bool,
    now_utc: datetime,
) -> dict[str, object]:
    policy = load_retention_config(config_path)
    archive_root = base_dir / str(policy["archive_dir"])
    archive_root.mkdir(parents=True, exist_ok=True)
    index_path = archive_root / "index.json"

    actions, scanned_files = build_retention_plan(base_dir=base_dir, policy=policy, now_utc=now_utc)
    groups: dict[tuple[str, str], list[Path]] = {}
    for action in actions:
        date_key = datetime.fromtimestamp(action.path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d")
        groups.setdefault((action.group, date_key), []).append(action.path)

    summary: dict[str, object] = {
        "scanned_files": scanned_files,
        "candidate_files": len(actions),
        "zip_files_created": 0,
        "files_archived": 0,
        "files_deleted_after_archive": 0,
        "dry_run": dry_run,
        "delete_after_archive": delete_after_archive,
    }

    index_data = _load_index(index_path)
    archives_list = index_data.setdefault("archives", [])
    if not isinstance(archives_list, list):
        archives_list = []
        index_data["archives"] = archives_list

    run_stamp = now_utc.strftime("%H%M%S")
    for (group_name, date_key), paths in sorted(groups.items()):
        target_dir = archive_root / date_key
        zip_name = f"{group_name}_{run_stamp}.zip"
        zip_path = target_dir / zip_name
        if dry_run:
            summary["zip_files_created"] = int(summary["zip_files_created"]) + 1
            summary["files_archived"] = int(summary["files_archived"]) + len(paths)
            continue

        target_dir.mkdir(parents=True, exist_ok=True)
        with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED) as zf:
            for file_path in sorted(paths):
                arcname = file_path.relative_to(base_dir).as_posix()
                zf.write(file_path, arcname=arcname)
        checksum = _zip_sha256(zip_path)
        zip_size = zip_path.stat().st_size
        summary["zip_files_created"] = int(summary["zip_files_created"]) + 1
        summary["files_archived"] = int(summary["files_archived"]) + len(paths)

        entry = {
            "created_at_utc": now_utc.isoformat(),
            "date_key": date_key,
            "group": group_name,
            "zip_path": zip_path.relative_to(base_dir).as_posix(),
            "sha256": checksum,
            "file_count": len(paths),
            "size_bytes": zip_size,
        }
        archives_list.append(entry)

        if delete_after_archive:
            for file_path in paths:
                file_path.unlink(missing_ok=True)
            summary["files_deleted_after_archive"] = int(summary["files_deleted_after_archive"]) + len(paths)

    if not dry_run:
        index_data["generated_at_utc"] = now_utc.isoformat()
        with index_path.open("w", encoding="utf-8") as handle:
            json.dump(index_data, handle, indent=2, ensure_ascii=True)

    return summary


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now_utc = parse_now_utc(args.now_utc)
    summary = run_archive(
        base_dir=PROJECT_ROOT,
        config_path=PROJECT_ROOT / args.config,
        dry_run=parse_bool(args.dry_run),
        delete_after_archive=parse_bool(args.delete_after_archive),
        now_utc=now_utc,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
