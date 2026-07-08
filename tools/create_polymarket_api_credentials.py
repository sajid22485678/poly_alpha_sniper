"""Derive/create Polymarket CLOB API credentials from the wallet key in .env.

Run: python -m poly_alpha_sniper.tools.create_polymarket_api_credentials [--write-env]

- Requires POLYMARKET_PRIVATE_KEY in .env (the bot NEVER creates a wallet).
- Prints api key/secret/passphrase ONLY because you explicitly ran this tool.
- NEVER prints the private key or any signed payload.
- --write-env backs up .env to .env.backup FIRST, then updates only the
  POLYMARKET_API_* lines.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def _update_env(env_path: Path, values: dict[str, str]) -> None:
    backup = env_path.with_suffix(".backup")
    shutil.copy2(env_path, backup)
    print(f"backup written: {backup.name}")
    lines = env_path.read_text(encoding="utf-8").splitlines()
    seen = set()
    out = []
    for line in lines:
        key = line.split("=", 1)[0].strip() if "=" in line else ""
        if key in values:
            out.append(f"{key}={values[key]}")
            seen.add(key)
        else:
            out.append(line)
    for key, value in values.items():
        if key not in seen:
            out.append(f"{key}={value}")
    env_path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"updated {env_path.name} ({', '.join(values)})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-env", action="store_true",
                        help="write derived creds into .env (backs up .env first)")
    args = parser.parse_args()

    from poly_alpha_sniper.core.config_loader import PROJECT_ROOT, load_config, load_secrets
    cfg = load_config()
    secrets = load_secrets()
    if not secrets.has("POLYMARKET_PRIVATE_KEY"):
        raise SystemExit("POLYMARKET_PRIVATE_KEY not set in .env — this tool derives "
                         "API creds from YOUR existing wallet; it never creates one.")
    try:
        from py_clob_client.client import ClobClient  # type: ignore
    except ImportError:
        raise SystemExit("py-clob-client not installed: pip install py-clob-client")

    sig_raw = secrets.get("POLYMARKET_SIGNATURE_TYPE")
    try:
        signature_type = int(sig_raw) if sig_raw else 0
    except ValueError:
        signature_type = 0
    kwargs = {"host": cfg.polymarket.clob_base_url,
              "key": secrets.get("POLYMARKET_PRIVATE_KEY"),
              "chain_id": 137, "signature_type": signature_type}
    funder = secrets.get("POLYMARKET_FUNDER_ADDRESS")
    if funder:
        kwargs["funder"] = funder
    client = ClobClient(**kwargs)
    creds = client.create_or_derive_api_creds()

    print("\nDerived CLOB API credentials (printed because you explicitly ran this tool):")
    print(f"POLYMARKET_API_KEY={creds.api_key}")
    print(f"POLYMARKET_API_SECRET={creds.api_secret}")
    print(f"POLYMARKET_API_PASSPHRASE={creds.api_passphrase}")
    print("\n(The private key itself is never printed.)")

    if args.write_env:
        env_path = PROJECT_ROOT / ".env"
        if not env_path.exists():
            raise SystemExit(".env not found — copy .env.example to .env first")
        _update_env(env_path, {
            "POLYMARKET_API_KEY": creds.api_key,
            "POLYMARKET_API_SECRET": creds.api_secret,
            "POLYMARKET_API_PASSPHRASE": creds.api_passphrase})


if __name__ == "__main__":
    main()
