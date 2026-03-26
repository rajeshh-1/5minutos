from __future__ import annotations

from dataclasses import dataclass

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

from src.core.env_loader import resolve_env


@dataclass(frozen=True)
class PolymarketAuthConfig:
    api_key: str
    api_passphrase: str
    api_secret: str
    address: str
    private_key: str
    clob_host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    signature_type: int = 0
    funder: str = ""
    timeout_sec: float = 5.0
    retry_count: int = 2

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "PolymarketAuthConfig":
        values = resolve_env(
            aliases={
                "POLY_API_KEY": ("POLY_API_KEY",),
                "POLY_PASSPHRASE": ("POLY_PASSPHRASE", "POLY_API_PASSPHRASE"),
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
        import os

        clob_host = str(os.getenv("CLOB_HOST", "https://clob.polymarket.com")).strip() or "https://clob.polymarket.com"
        signature_type = int(str(os.getenv("POLY_SIGNATURE_TYPE", "0")).strip() or "0")
        funder = str(os.getenv("POLY_FUNDER", "")).strip()
        timeout_sec = float(str(os.getenv("POLY_TIMEOUT_SEC", "5")).strip() or "5")
        retry_count = int(str(os.getenv("POLY_RETRY_COUNT", "2")).strip() or "2")
        chain_id = int(str(os.getenv("POLY_CHAIN_ID", "137")).strip() or "137")
        return cls(
            api_key=values["POLY_API_KEY"],
            api_passphrase=values["POLY_PASSPHRASE"],
            api_secret=values["POLY_SECRET"],
            address=values["POLY_ADDRESS"],
            private_key=values["POLY_PRIVATE_KEY"],
            clob_host=clob_host.rstrip("/"),
            chain_id=chain_id,
            signature_type=signature_type,
            funder=funder,
            timeout_sec=timeout_sec,
            retry_count=retry_count,
        )


def build_clob_client(cfg: PolymarketAuthConfig) -> ClobClient:
    client = ClobClient(
        host=cfg.clob_host,
        chain_id=int(cfg.chain_id),
        key=cfg.private_key,
        signature_type=int(cfg.signature_type),
        funder=(cfg.funder or None),
    )
    creds = ApiCreds(
        api_key=cfg.api_key,
        api_secret=cfg.api_secret,
        api_passphrase=cfg.api_passphrase,
    )
    client.set_api_creds(creds)
    return client
