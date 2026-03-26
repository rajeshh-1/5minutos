from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL

from src.execution.polymarket_auth import PolymarketAuthConfig, build_clob_client


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass
class LiveOrderResult:
    success: bool
    order_id: str
    status: str
    filled_size: float
    avg_price: float
    latency_ms: int
    error: str = ""


class PolymarketLiveExecutor:
    def __init__(self, *, auth: PolymarketAuthConfig | None, dry_run: bool = True) -> None:
        self.auth = auth
        self.dry_run = bool(dry_run)
        self.client = None
        self._open_order_ids: set[str] = set()
        if not self.dry_run:
            if self.auth is None:
                raise ValueError("auth is required when dry_run=false")
            self.client = build_clob_client(self.auth)

    def _parse_order_id(self, payload: Any) -> str:
        if isinstance(payload, dict):
            for key in ("orderID", "order_id", "id"):
                value = payload.get(key)
                if value:
                    return str(value)
            nested = payload.get("order")
            if isinstance(nested, dict):
                for key in ("orderID", "order_id", "id"):
                    value = nested.get(key)
                    if value:
                        return str(value)
        return ""

    def _parse_order_status(self, payload: Any) -> tuple[str, float, float]:
        if not isinstance(payload, dict):
            return "unknown", 0.0, 0.0
        status = (
            str(payload.get("status") or payload.get("order_status") or payload.get("state") or "unknown")
            .strip()
            .lower()
        )
        filled_size = _safe_float(
            payload.get("filled_size")
            or payload.get("filled")
            or payload.get("matched_amount")
            or payload.get("size_matched")
            or 0.0,
            default=0.0,
        )
        avg_price = _safe_float(
            payload.get("avg_price")
            or payload.get("price")
            or payload.get("matched_price")
            or 0.0,
            default=0.0,
        )
        return status, filled_size, avg_price

    def _post_limit_order(self, *, token_id: str, price: float, size: float, side: str) -> LiveOrderResult:
        started = time.time()
        if self.dry_run:
            fake_id = f"dry_{int(started * 1000)}"
            return LiveOrderResult(
                success=True,
                order_id=fake_id,
                status="filled",
                filled_size=float(size),
                avg_price=float(price),
                latency_ms=0,
            )

        if self.client is None:
            return LiveOrderResult(
                success=False,
                order_id="",
                status="error",
                filled_size=0.0,
                avg_price=0.0,
                latency_ms=0,
                error="client_not_initialized",
            )

        order_args = OrderArgs(
            token_id=str(token_id),
            price=float(price),
            size=float(size),
            side=str(side),
        )
        try:
            signed_order = self.client.create_order(order_args)
            posted = self.client.post_order(signed_order, orderType="GTC", post_only=False)
            order_id = self._parse_order_id(posted)
            if order_id:
                self._open_order_ids.add(order_id)
            latency_ms = int((time.time() - started) * 1000)
            if not order_id:
                return LiveOrderResult(
                    success=False,
                    order_id="",
                    status="error",
                    filled_size=0.0,
                    avg_price=0.0,
                    latency_ms=latency_ms,
                    error=f"missing_order_id response={posted}",
                )
            status_payload = self.client.get_order(order_id)
            status, filled_size, avg_price = self._parse_order_status(status_payload)
            return LiveOrderResult(
                success=True,
                order_id=order_id,
                status=status,
                filled_size=filled_size,
                avg_price=avg_price if avg_price > 0 else float(price),
                latency_ms=latency_ms,
            )
        except Exception as exc:
            latency_ms = int((time.time() - started) * 1000)
            return LiveOrderResult(
                success=False,
                order_id="",
                status="error",
                filled_size=0.0,
                avg_price=0.0,
                latency_ms=latency_ms,
                error=str(exc),
            )

    def place_leg1_order(self, *, token_id: str, price: float, size: float) -> LiveOrderResult:
        return self._post_limit_order(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY,
        )

    def place_leg2_hedge(self, *, token_id: str, price: float, size: float, timeout_ms: int) -> LiveOrderResult:
        started = time.time()
        placed = self._post_limit_order(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY,
        )
        if not placed.success or self.dry_run:
            placed.latency_ms = int((time.time() - started) * 1000)
            return placed

        if self.client is None:
            return LiveOrderResult(
                success=False,
                order_id=placed.order_id,
                status="error",
                filled_size=0.0,
                avg_price=0.0,
                latency_ms=int((time.time() - started) * 1000),
                error="client_not_initialized",
            )

        deadline = (time.time() * 1000.0) + float(timeout_ms)
        last_status = placed.status or "submitted"
        filled_size = float(placed.filled_size)
        avg_price = float(placed.avg_price)
        while (time.time() * 1000.0) <= deadline:
            try:
                status_payload = self.client.get_order(placed.order_id)
                last_status, filled_size, status_avg_price = self._parse_order_status(status_payload)
                if status_avg_price > 0:
                    avg_price = status_avg_price
            except Exception:
                pass
            if "fill" in last_status and "partial" not in last_status:
                return LiveOrderResult(
                    success=True,
                    order_id=placed.order_id,
                    status=last_status,
                    filled_size=filled_size if filled_size > 0 else float(size),
                    avg_price=avg_price if avg_price > 0 else float(price),
                    latency_ms=int((time.time() - started) * 1000),
                )
            time.sleep(0.15)

        try:
            self.client.cancel(placed.order_id)
        except Exception:
            pass
        return LiveOrderResult(
            success=False,
            order_id=placed.order_id,
            status=last_status or "timeout",
            filled_size=filled_size,
            avg_price=avg_price if avg_price > 0 else float(price),
            latency_ms=int((time.time() - started) * 1000),
            error="hedge_timeout",
        )

    def unwind_leg1(self, *, token_id: str, price: float, size: float) -> LiveOrderResult:
        return self._post_limit_order(
            token_id=token_id,
            price=price,
            size=size,
            side=SELL,
        )

    def get_order_status(self, order_id: str) -> dict[str, Any]:
        if not order_id:
            return {"status": "missing_order_id"}
        if self.dry_run:
            return {
                "id": order_id,
                "status": "filled",
                "filled_size": 1.0,
                "avg_price": 0.5,
            }
        if self.client is None:
            return {"id": order_id, "status": "error", "error": "client_not_initialized"}
        try:
            payload = self.client.get_order(order_id)
            if isinstance(payload, dict):
                return payload
            return {"id": order_id, "status": "unknown", "raw": str(payload)}
        except Exception as exc:
            return {"id": order_id, "status": "error", "error": str(exc)}

    def cancel_open_orders(self, order_ids: list[str] | None = None) -> dict[str, Any]:
        if self.dry_run:
            return {"cancelled": list(order_ids or []), "errors": []}
        if self.client is None:
            return {"cancelled": [], "errors": ["client_not_initialized"]}

        target_ids = [str(x) for x in (order_ids or list(self._open_order_ids)) if str(x).strip()]
        cancelled: list[str] = []
        errors: list[str] = []
        for order_id in target_ids:
            try:
                self.client.cancel(order_id)
                cancelled.append(order_id)
                self._open_order_ids.discard(order_id)
            except Exception as exc:
                errors.append(f"{order_id}:{exc}")
        return {"cancelled": cancelled, "errors": errors}
