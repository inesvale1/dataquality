"""Diagnóstico de conexão Oracle — executa fora do framework para isolar o problema."""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    package_parent = Path(__file__).resolve().parents[2]
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))


def main() -> None:
    import keyring

    service = "dataquality-oracle"
    username = "sefaz2\\49756615"

    # ── 1. Verifica keyring ────────────────────────────────────────────────
    print(f"\n[1] Lendo senha do keyring  service={service!r}  username={username!r}")
    password = keyring.get_password(service, username)
    if password is None:
        print("    ERRO: senha não encontrada no keyring.")
        print("    Execute:  python scripts/store_keyring_secret.py --service dataquality-oracle --username 'sefaz2\\49756615' --show-check")
        sys.exit(1)

    masked = password[:2] + "*" * (len(password) - 2) if len(password) > 2 else "**"
    print(f"    Senha encontrada: {masked}  (len={len(password)})")

    # ── 2. Conexão direta via oracledb (sem SQLAlchemy) ───────────────────
    host = "cpscanx.sefaz-ce.gov.br"
    port = 1521
    service_name = "EXAPROD"

    print(f"\n[2] Tentando oracledb.connect direto  user={username!r}  host={host}  port={port}  service_name={service_name!r}")
    try:
        import oracledb
        conn = oracledb.connect(user=username, password=password, host=host, port=port, service_name=service_name)
        print("    SUCESSO na conexão direta oracledb!")
        row = conn.cursor().execute("SELECT 1 FROM DUAL").fetchone()
        print(f"    SELECT 1 FROM DUAL = {row}")
        conn.close()
    except Exception as exc:
        print(f"    FALHOU: {exc}")

    # ── 3. Conexão via SQLAlchemy + connect_args ──────────────────────────
    print("\n[3] Tentando SQLAlchemy oracle+oracledb:// com connect_args")
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(
            "oracle+oracledb://",
            connect_args={
                "user": username,
                "password": password,
                "host": host,
                "port": port,
                "service_name": service_name,
            },
        )
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 FROM DUAL")).scalar()
            print(f"    SUCESSO via SQLAlchemy!  SELECT 1 = {result}")
        engine.dispose()
    except Exception as exc:
        print(f"    FALHOU: {exc}")

    # ── 4. Conexão via SQLAlchemy com DSN composto ────────────────────────
    print("\n[4] Tentando SQLAlchemy com DSN  host:port/service")
    try:
        from sqlalchemy import create_engine, text
        dsn = f"{host}:{port}/{service_name}"
        engine = create_engine(
            "oracle+oracledb://",
            connect_args={"user": username, "password": password, "dsn": dsn},
        )
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 FROM DUAL")).scalar()
            print(f"    SUCESSO via DSN!  SELECT 1 = {result}")
        engine.dispose()
    except Exception as exc:
        print(f"    FALHOU: {exc}")


if __name__ == "__main__":
    main()
