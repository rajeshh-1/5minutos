from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: str | Path = ".env") -> dict[str, str]:
    env_path = Path(path)
    if not env_path.exists():
        return {}

    parsed: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
            value = value[1:-1]
        parsed[key] = value
    return parsed


def resolve_env(
    *,
    aliases: dict[str, tuple[str, ...]],
    required: tuple[str, ...],
    env_file: str | Path = ".env",
) -> dict[str, str]:
    loaded = load_env_file(env_file)
    merged: dict[str, str] = dict(loaded)
    for key, value in os.environ.items():
        if key not in merged and value:
            merged[key] = value

    resolved: dict[str, str] = {}
    missing: list[str] = []
    for canonical in required:
        candidates = aliases.get(canonical, (canonical,))
        found = ""
        for candidate in candidates:
            value = str(merged.get(candidate, "")).strip()
            if value:
                found = value
                break
        if not found:
            missing.append(canonical)
            continue
        resolved[canonical] = found

    if missing:
        raise ValueError(
            "Missing required environment variables: " + ", ".join(sorted(missing))
        )

    for key, value in resolved.items():
        os.environ.setdefault(key, value)
    return resolved
