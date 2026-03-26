from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from scripts.generate_synthetic_sol5m_data import generate_synthetic_rows, write_synthetic_csv


def test_synthetic_generator_outputs_expected_columns_and_rows(tmp_path: Path) -> None:
    rows = generate_synthetic_rows(
        minutes=2,
        seed=42,
        regime="normal",
        start_utc=datetime(2026, 3, 25, 0, 0, tzinfo=timezone.utc),
    )
    assert len(rows) == 120
    out_file = write_synthetic_csv(rows=rows, out_file=tmp_path / "synthetic.csv")
    assert out_file.exists()

    with out_file.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        header = list(reader.fieldnames or [])
        got_rows = list(reader)
    assert len(got_rows) == 120
    assert header == [
        "timestamp_utc",
        "market_key",
        "up_price",
        "down_price",
        "seconds_to_close",
        "synthetic_depth",
        "synthetic_volatility",
    ]


def test_synthetic_generator_is_reproducible_with_same_seed(tmp_path: Path) -> None:
    start = datetime(2026, 3, 25, 0, 0, tzinfo=timezone.utc)
    rows_a = generate_synthetic_rows(minutes=3, seed=99, regime="mixed", start_utc=start)
    rows_b = generate_synthetic_rows(minutes=3, seed=99, regime="mixed", start_utc=start)
    file_a = write_synthetic_csv(rows=rows_a, out_file=tmp_path / "a.csv")
    file_b = write_synthetic_csv(rows=rows_b, out_file=tmp_path / "b.csv")
    assert file_a.read_text(encoding="utf-8") == file_b.read_text(encoding="utf-8")
