"""Authenticated Polymarket CLOB trading client — LIVE GATED.

The ONLY construction path is create_live_client(..., live_gates_ok=True);
anything else raises. Wraps the official `py-clob-client` SDK behind a lazy
import so shadow/simulation/backtest never need it installed, and keeps the
whole SDK surface inside this one module so it can be swapped when Polymarket
ships client changes.

SECURITY: never logs key material; every raised error message passes through
redact_text. Nothing here prints, stores or transmits secrets.

ASSUMPTIONS (validate against current Polymarket docs when going live):
- POLYMARKET_SIGNATURE_TYPE: 0 = EOA wallet, 1 = Magic/email proxy,
  2 = browser/Gnosis proxy. Default 0 when unset.
- USDC balances use 6 decimals via the balance-allowance endpoint.
- Positions come from the public data API keyed by the funder address.
"""
from __future__ import annotations

from typing import Any

import aiohttp

from poly_alpha_sniper.core.contracts import (
    OrderRecord, OrderRequest, OrderSide, OrderState, Outcome, Position)
from poly_alpha_sniper.core.logger import get_logger, redact_text

log = get_logger("clob_private")

DATA_API = "https://data-api.polymarket.com"


def create_live_client(cfg, secrets, live_gates_ok: bool):
    if live_gates_ok is not True:
        raise RuntimeError("live gates not passed — refusing to construct trading client")
    if not secrets.has("POLYMARKET_PRIVATE_KEY"):
        raise RuntimeError("POLYMARKET_PRIVATE_KEY missing from .env")
    return _LivePolymarketClient(cfg, secrets)


class _LivePolymarketClient:
    """Implements contracts.ClobTradingClient over py-clob-client."""

    def __init__(self, cfg, secrets):
        self.cfg = cfg
        self.secrets = secrets
        self._client = self._build_sdk_client()

    def _build_sdk_client(self):
        try:
            from py_clob_client.client import ClobClient  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "py-clob-client is required for live trading: pip install py-clob-client"
            ) from exc
        sig_raw = self.secrets.get("POLYMARKET_SIGNATURE_TYPE")
        try:
            signature_type = int(sig_raw) if sig_raw else 0
        except ValueError:
            signature_type = 0
        kwargs: dict[str, Any] = {
            "host": self.cfg.polymarket.clob_base_url,
            "key": self.secrets.get("POLYMARKET_PRIVATE_KEY"),
            "chain_id": 137,  # Polygon mainnet
            "signature_type": signature_type,
        }
        funder = self.secrets.get("POLYMARKET_FUNDER_ADDRESS")
        if funder:
            kwargs["funder"] = funder
        try:
            client = ClobClient(**kwargs)
            if self.secrets.has("POLYMARKET_API_KEY"):
                from py_clob_client.clob_types import ApiCreds  # type: ignore
                client.set_api_creds(ApiCreds(
                    api_key=self.secrets.get("POLYMARKET_API_KEY"),
                    api_secret=self.secrets.get("POLYMARKET_API_SECRET"),
                    api_passphrase=self.secrets.get("POLYMARKET_API_PASSPHRASE")))
            else:
                client.set_api_creds(client.create_or_derive_api_creds())
            return client
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("CLOB client init failed: "
                               + redact_text(repr(exc))[:200]) from exc

    # ------------------------------------------------------------------
    async def place_order(self, req: OrderRequest) -> OrderRecord:
        import asyncio
        return await asyncio.to_thread(self._place_order_sync, req)

    def _place_order_sync(self, req: OrderRequest) -> OrderRecord:
        from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore
        from py_clob_client.order_builder.constants import BUY, SELL  # type: ignore
        side = BUY if req.side.is_buy else SELL
        args = OrderArgs(token_id=req.token_id, price=req.price,
                         size=req.size_shares, side=side)
        record = OrderRecord(order_id=req.order_id, token_id=req.token_id,
                             market_id=req.market_id, side=req.side,
                             price=req.price, size_shares=req.size_shares,
                             size_usd=req.size_usd, state=OrderState.SUBMITTED)
        try:
            signed = self._client.create_order(args)
            resp = self._client.post_order(signed, OrderType.GTC)
        except Exception as exc:  # noqa: BLE001
            record.state = OrderState.FAILED
            record.error = redact_text(repr(exc))[:200]
            log.error("order_submit_failed", extra={"extra": {"error": record.error}})
            return record
        record.exchange_order_id = str(resp.get("orderID") or resp.get("orderId") or "")
        status = str(resp.get("status", "")).lower()
        taking = float(resp.get("takingAmount") or 0)
        making = float(resp.get("makingAmount") or 0)
        if status == "matched":
            record.state = OrderState.MATCHED
            if req.side.is_buy and making > 0:
                record.filled_shares = taking
                record.avg_fill_price = making / taking if taking > 0 else req.price
            elif making > 0:
                record.filled_shares = making
                record.avg_fill_price = taking / making if making > 0 else req.price
            else:
                record.filled_shares = req.size_shares
                record.avg_fill_price = req.price
        elif status in ("live", "open", "delayed"):
            record.state = OrderState.OPEN
        else:
            record.state = OrderState.FAILED
            record.error = f"unexpected status {status}"
        return record

    async def cancel_order(self, order_id: str) -> bool:
        import asyncio
        try:
            resp = await asyncio.to_thread(self._client.cancel, order_id)
            return bool(resp)
        except Exception as exc:  # noqa: BLE001
            log.error("cancel_failed", extra={"extra": {"error": redact_text(repr(exc))[:150]}})
            return False

    async def cancel_all(self) -> int:
        import asyncio
        try:
            resp = await asyncio.to_thread(self._client.cancel_all)
            cancelled = resp.get("canceled") if isinstance(resp, dict) else resp
            return len(cancelled) if isinstance(cancelled, list) else int(bool(resp))
        except Exception as exc:  # noqa: BLE001
            log.error("cancel_all_failed", extra={"extra": {"error": redact_text(repr(exc))[:150]}})
            return 0

    async def get_open_orders(self) -> list[OrderRecord]:
        import asyncio
        try:
            raw = await asyncio.to_thread(self._client.get_orders)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("open orders fetch failed: "
                               + redact_text(repr(exc))[:150]) from exc
        out: list[OrderRecord] = []
        for o in raw or []:
            try:
                side = OrderSide.BUY_YES if str(o.get("side", "")).upper() == "BUY" \
                    else OrderSide.SELL_YES
                out.append(OrderRecord(
                    order_id=str(o.get("id") or ""), exchange_order_id=str(o.get("id") or ""),
                    token_id=str(o.get("asset_id") or ""), market_id=str(o.get("market") or ""),
                    side=side, price=float(o.get("price") or 0),
                    size_shares=float(o.get("original_size") or 0),
                    filled_shares=float(o.get("size_matched") or 0),
                    state=OrderState.OPEN))
            except (TypeError, ValueError):
                continue
        return out

    async def get_balance_usd(self) -> float:
        import asyncio
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams  # type: ignore
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            resp = await asyncio.to_thread(self._client.get_balance_allowance, params)
            return float(resp.get("balance", 0)) / 1e6  # USDC 6 decimals
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("balance fetch failed: "
                               + redact_text(repr(exc))[:150]) from exc

    async def get_positions(self) -> list[Position]:
        funder = self.secrets.get("POLYMARKET_FUNDER_ADDRESS")
        if not funder:
            return []
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)) as session:
            async with session.get(f"{DATA_API}/positions",
                                   params={"user": funder}) as resp:
                resp.raise_for_status()
                raw = await resp.json()
        out: list[Position] = []
        for p in raw or []:
            try:
                shares = float(p.get("size") or 0)
                if shares <= 0:
                    continue
                outcome = Outcome.YES if str(p.get("outcome", "")).lower() in (
                    "yes", "up", "above") else Outcome.NO
                out.append(Position(
                    token_id=str(p.get("asset") or p.get("tokenId") or ""),
                    market_id=str(p.get("conditionId") or ""),
                    outcome=outcome, shares=shares,
                    avg_entry_price=float(p.get("avgPrice") or 0)))
            except (TypeError, ValueError):
                continue
        return out
