from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Any

from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, OrderArgs
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
    _SUPPORTED_ORDER_TYPES = {"GTC", "FOK", "FAK"}
    _EPS = 1e-9

    def __init__(self, *, auth: PolymarketAuthConfig | None, dry_run: bool = True) -> None:
        self.auth = auth
        self.dry_run = bool(dry_run)
        self.client = None
        self._open_order_ids: set[str] = set()
        if not self.dry_run:
            if self.auth is None:
                raise ValueError("auth is required when dry_run=false")
            self.client = build_clob_client(self.auth)

    def preflight(self) -> dict[str, Any]:
        if self.dry_run:
            return {"ok": True, "mode": "dry_run"}
        if self.client is None:
            return {"ok": False, "error": "client_not_initialized"}

        result: dict[str, Any] = {"ok": False}
        creds_source = "env"
        try:
            result["get_ok"] = self.client.get_ok()
            self.client.assert_level_1_auth()
            self.client.assert_level_2_auth()
            if self.auth is not None:
                params = BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    token_id="",
                    signature_type=int(self.auth.signature_type),
                )
                try:
                    _ = self.client.get_balance_allowance(params)
                except Exception as exc:
                    message = str(exc)
                    if ("Unauthorized/Invalid api key" not in message) and ("status_code=401" not in message):
                        raise
                    self.client.set_api_creds(self.client.create_or_derive_api_creds())
                    self.client.assert_level_2_auth()
                    _ = self.client.get_balance_allowance(params)
                    creds_source = "derived"
                result["balance_allowance"] = "ok"
            result["creds_source"] = creds_source
            result["ok"] = True
            return result
        except Exception as exc:
            result["error"] = str(exc)
            return result

    def _normalize_order_type(self, value: str) -> str:
        order_type = str(value or "FAK").strip().upper()
        if order_type not in self._SUPPORTED_ORDER_TYPES:
            return "FAK"
        return order_type

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

    @classmethod
    def _is_filled(
        cls,
        status: str,
        filled_size: float,
        *,
        requested_size: float,
        min_fill_ratio: float,
    ) -> bool:
        status_norm = str(status or "").strip().lower()
        req = max(0.0, float(requested_size))
        ratio = max(0.0, min(1.0, float(min_fill_ratio)))
        positive_status = {"filled", "matched", "partially_filled", "partiallyfilled"}
        if req <= cls._EPS:
            return float(filled_size) > cls._EPS or (status_norm in positive_status)
        required = req * ratio
        if float(filled_size) + cls._EPS >= required:
            return True
        # Be conservative: do not treat "confirmed/mined" as fill confirmation.
        # We only proceed when size_matched/filled_size is positive or explicit filled status appears.
        return status_norm in positive_status

    def _candidate_sizes(self, *, side: str, price: float, size: float) -> list[float]:
        candidates: list[float] = []

        def _add(value: float) -> None:
            val = float(value)
            if val <= 0:
                return
            if all(abs(val - existing) > 1e-9 for existing in candidates):
                candidates.append(val)

        try:
            price_d = Decimal(str(round(float(price), 4)))
            size_d = Decimal(str(float(size)))
        except (InvalidOperation, ValueError):
            return [max(0.0001, float(size))]

        size_4 = size_d.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
        size_2 = size_d.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        _add(float(size_4))
        _add(float(size_2))

        # For BUY orders, CLOB may validate maker amount with max 2 decimals
        # and requires marketable BUY notional >= $1.
        # Build alternatives from 2-decimal notionals, converting to 4-decimal size.
        if str(side).strip().upper() == str(BUY).upper() and price_d > 0:
            min_notional = Decimal("1.00")
            requested_notional = (price_d * size_d).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            base_notional = max(min_notional, requested_notional)
            for bump in (Decimal("0.00"), Decimal("0.01"), Decimal("0.02"), Decimal("0.03")):
                notional = base_notional + bump
                sized = (notional / price_d).quantize(Decimal("0.0001"), rounding=ROUND_UP)
                _add(float(sized))

        if not candidates:
            return [max(0.0001, float(size))]
        return candidates

    def _post_order(
        self,
        *,
        token_id: str,
        price: float,
        size: float,
        side: str,
        order_type: str = "FAK",
        post_only: bool = False,
        require_fill: bool = False,
        fill_timeout_ms: int = 0,
        min_fill_ratio: float = 0.0001,
    ) -> LiveOrderResult:
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
            price=float(round(float(price), 4)),
            size=float(round(float(size), 4)),
            side=str(side),
        )
        normalized_order_type = self._normalize_order_type(order_type)
        candidate_sizes = self._candidate_sizes(
            side=str(side),
            price=float(order_args.price),
            size=float(size),
        )

        last_error = ""
        for index, candidate_size in enumerate(candidate_sizes):
            order_args.size = float(candidate_size)
            try:
                signed_order = self.client.create_order(order_args)
                posted = self.client.post_order(
                    signed_order,
                    orderType=normalized_order_type,
                    post_only=bool(post_only),
                )
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
                if require_fill:
                    deadline_ms = (time.time() * 1000.0) + max(0, int(fill_timeout_ms))
                    while not self._is_filled(
                        status,
                        filled_size,
                        requested_size=float(order_args.size),
                        min_fill_ratio=float(min_fill_ratio),
                    ) and (time.time() * 1000.0) < deadline_ms:
                        time.sleep(0.15)
                        status_payload = self.client.get_order(order_id)
                        status, filled_size, avg_price = self._parse_order_status(status_payload)
                    if not self._is_filled(
                        status,
                        filled_size,
                        requested_size=float(order_args.size),
                        min_fill_ratio=float(min_fill_ratio),
                    ):
                        try:
                            self.client.cancel(order_id)
                        except Exception:
                            pass
                        return LiveOrderResult(
                            success=False,
                            order_id=order_id,
                            status=status or "unfilled",
                            filled_size=filled_size,
                            avg_price=avg_price if avg_price > 0 else float(price),
                            latency_ms=int((time.time() - started) * 1000),
                            error="order_not_filled",
                        )
                return LiveOrderResult(
                    success=True,
                    order_id=order_id,
                    status=status,
                    filled_size=filled_size,
                    avg_price=avg_price if avg_price > 0 else float(price),
                    latency_ms=latency_ms,
                )
            except Exception as exc:
                last_error = str(exc)
                can_retry_precision = (
                    index < (len(candidate_sizes) - 1)
                    and "invalid amounts" in last_error.lower()
                )
                if can_retry_precision:
                    continue
                break

        latency_ms = int((time.time() - started) * 1000)
        return LiveOrderResult(
            success=False,
            order_id="",
            status="error",
            filled_size=0.0,
            avg_price=0.0,
            latency_ms=latency_ms,
            error=last_error or "order_submit_failed",
        )

    def place_leg1_order(
        self,
        *,
        token_id: str,
        price: float,
        size: float,
        order_type: str = "FAK",
        timeout_ms: int = 1200,
    ) -> LiveOrderResult:
        order_type_norm = self._normalize_order_type(order_type)
        return self._post_order(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY,
            order_type=order_type_norm,
            post_only=False,
            require_fill=True,
            fill_timeout_ms=timeout_ms,
            min_fill_ratio=(0.999 if order_type_norm == "FOK" else 0.0001),
        )

    def place_leg2_hedge(
        self,
        *,
        token_id: str,
        price: float,
        size: float,
        timeout_ms: int,
        order_type: str = "FAK",
        post_only: bool = False,
        require_fill: bool = True,
    ) -> LiveOrderResult:
        return self._post_order(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY,
            order_type=order_type,
            post_only=bool(post_only),
            require_fill=bool(require_fill),
            fill_timeout_ms=int(timeout_ms),
            min_fill_ratio=0.0001,
        )

    def unwind_leg1(
        self,
        *,
        token_id: str,
        price: float,
        size: float,
        order_type: str = "FAK",
        timeout_ms: int = 1200,
    ) -> LiveOrderResult:
        return self._post_order(
            token_id=token_id,
            price=price,
            size=size,
            side=SELL,
            order_type=order_type,
            post_only=False,
            require_fill=True,
            fill_timeout_ms=timeout_ms,
            min_fill_ratio=0.999,
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

    def get_token_available_balance(self, token_id: str) -> float:
        if not token_id or self.dry_run or self.client is None:
            return 0.0
        signature_type = int(self.auth.signature_type) if self.auth is not None else 0
        params = BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=str(token_id),
            signature_type=signature_type,
        )
        try:
            payload = self.client.get_balance_allowance(params)
        except Exception as exc:
            message = str(exc)
            if ("Unauthorized/Invalid api key" in message) or ("status_code=401" in message):
                try:
                    self.client.set_api_creds(self.client.create_or_derive_api_creds())
                    payload = self.client.get_balance_allowance(params)
                except Exception:
                    return 0.0
            else:
                return 0.0
        if not isinstance(payload, dict):
            return 0.0
        raw_balance = _safe_float(payload.get("balance"), default=0.0)
        if raw_balance <= 0:
            return 0.0
        # CLOB conditional balances are returned in 1e6 base units.
        return float(raw_balance) / 1_000_000.0

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
