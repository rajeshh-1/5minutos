from __future__ import annotations

import csv
import json
from pathlib import Path

from src.core.config import load_strategy_config


def _write_replay_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["timestamp_utc", "market_key", "up_price", "down_price", "seconds_to_close"],
        )
        writer.writeheader()
        for i in range(1, 16):
            writer.writerow(
                {
                    "timestamp_utc": f"2026-03-25T00:00:{i:02d}Z",
                    "market_key": "SOL5M_TEST",
                    "up_price": 0.40 if i % 2 == 0 else 0.42,
                    "down_price": 0.58 if i % 2 == 0 else 0.60,
                    "seconds_to_close": 120,
                }
            )


def test_tuning_generates_required_artifacts(tmp_path: Path) -> None:
    from scripts.tune_params import build_coarse_candidates, main

    replay_file = tmp_path / "replay.csv"
    _write_replay_csv(replay_file)
    out_dir = tmp_path / "reports" / "tuning"
    cfg = load_strategy_config(Path(__file__).resolve().parents[1] / "configs" / "paper_sol5m_usd1.json")
    coarse = build_coarse_candidates(cfg, limit=6)
    assert len(coarse) == 6
    assert len({c.config_id for c in coarse}) == 6

    rc = main(
        [
            "--input",
            str(replay_file),
            "--config",
            str(Path(__file__).resolve().parents[1] / "configs" / "paper_sol5m_usd1.json"),
            "--out-dir",
            str(out_dir),
            "--seed-list",
            "42,99",
            "--coarse-limit",
            "4",
            "--top-k",
            "2",
            "--refine-per-top",
            "2",
            "--event-cap",
            "12",
        ]
    )
    assert rc == 0
    assert (out_dir / "tuning_results.csv").exists()
    assert (out_dir / "tuning_results.json").exists()
    assert (out_dir / "top_configs.json").exists()


def test_tuning_ranking_is_deterministic_for_same_inputs(tmp_path: Path) -> None:
    from scripts.tune_params import main

    replay_file = tmp_path / "replay.csv"
    _write_replay_csv(replay_file)
    out_a = tmp_path / "run_a"
    out_b = tmp_path / "run_b"
    base_args = [
        "--input",
        str(replay_file),
        "--config",
        str(Path(__file__).resolve().parents[1] / "configs" / "paper_sol5m_usd1.json"),
        "--seed-list",
        "42,99",
        "--coarse-limit",
        "4",
        "--top-k",
        "2",
        "--refine-per-top",
        "2",
        "--event-cap",
        "12",
    ]
    assert main([*base_args, "--out-dir", str(out_a)]) == 0
    assert main([*base_args, "--out-dir", str(out_b)]) == 0

    top_a = json.loads((out_a / "top_configs.json").read_text(encoding="utf-8"))
    top_b = json.loads((out_b / "top_configs.json").read_text(encoding="utf-8"))
    assert len(top_a) > 0
    assert len(top_b) > 0
    assert top_a[0]["config_id"] == top_b[0]["config_id"]
