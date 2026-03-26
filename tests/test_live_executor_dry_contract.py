from __future__ import annotations

from pathlib import Path

from src.core.env_loader import resolve_env
from src.execution.polymarket_live_executor import PolymarketLiveExecutor


def test_env_loader_resolves_aliases(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "POLY_API_KEY=key_x",
                "POLY_PASSPHRASE=pass_x",
                "POLY_API_SECRET=secret_alias",
                "POLY_FUNDER=0xabc",
                "POLY_PRIVATE_KEY=pk_x",
            ]
        ),
        encoding="utf-8",
    )

    resolved = resolve_env(
        aliases={
            "POLY_API_KEY": ("POLY_API_KEY",),
            "POLY_PASSPHRASE": ("POLY_PASSPHRASE",),
            "POLY_SECRET": ("POLY_SECRET", "POLY_API_SECRET"),
            "POLY_ADDRESS": ("POLY_ADDRESS", "POLY_FUNDER"),
            "POLY_PRIVATE_KEY": ("POLY_PRIVATE_KEY",),
        },
        required=(
            "POLY_API_KEY",
            "POLY_PASSPHRASE",
            "POLY_SECRET",
            "POLY_ADDRESS",
            "POLY_PRIVATE_KEY",
        ),
        env_file=env_file,
    )
    assert resolved["POLY_SECRET"] == "secret_alias"
    assert resolved["POLY_ADDRESS"] == "0xabc"


def test_live_executor_dry_contract() -> None:
    ex = PolymarketLiveExecutor(auth=None, dry_run=True)
    leg1 = ex.place_leg1_order(token_id="token_a", price=0.42, size=2.0)
    assert leg1.success
    assert leg1.order_id.startswith("dry_")
    assert leg1.status == "filled"

    leg2 = ex.place_leg2_hedge(token_id="token_b", price=0.53, size=2.0, timeout_ms=2000)
    assert leg2.success
    assert leg2.order_id.startswith("dry_")

    unwind = ex.unwind_leg1(token_id="token_a", price=0.40, size=2.0)
    assert unwind.success
    assert unwind.status == "filled"

    status = ex.get_order_status(leg1.order_id)
    assert str(status.get("status")) == "filled"

    cancelled = ex.cancel_open_orders([leg1.order_id, leg2.order_id])
    assert isinstance(cancelled.get("cancelled"), list)
