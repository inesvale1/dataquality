from __future__ import annotations

import math

import pandas as pd

from dataquality.infrastructure.io.secure_credentials import (
    DatabaseConnectionSettings,
    build_database_engine,
)

_DEFAULT_RESULTS_SCHEMA = "qualidade_dados"


class PostgreSQLResultExporter:
    """Writes framework output to a PostgreSQL (AWS RDS) schema.

    Target tables (must exist — see config/ddl_output_schema_postgresql.sql):
      <schema>.dimensao
      <schema>.execucao          (SERIAL PK)
      <schema>.resultado_qualidade  (SERIAL PK)
      <schema>.problema_execucao    (SERIAL PK)

    Uses RETURNING to retrieve auto-generated PKs (no explicit sequences).
    """

    def __init__(
        self,
        connection_settings: DatabaseConnectionSettings,
        schema: str = _DEFAULT_RESULTS_SCHEMA,
    ):
        self.connection_settings = connection_settings
        self._results_schema = str(schema).strip().lower() if schema else _DEFAULT_RESULTS_SCHEMA
        self._engine = None
        self._dim_codes: dict[str, int] | None = None

    # ------------------------------------------------------------------
    # Public API  (same interface as OracleResultExporter)
    # ------------------------------------------------------------------

    def begin_execution(
        self,
        owner: str,
        source_type: str = "ATHENA",
        notes: str | None = None,
    ) -> int:
        from sqlalchemy import text

        with self._get_engine().begin() as conn:
            row = conn.execute(
                text(
                    f"INSERT INTO {self._results_schema}.execucao "
                    "(dsc_owner, tip_fonte, sta_execucao, dat_execucao, dsc_observacao) "
                    "VALUES (:owner, :fonte, 'RUNNING', CURRENT_TIMESTAMP, :obs) "
                    "RETURNING seq_execucao"
                ),
                {
                    "owner": _str(owner, 128),
                    "fonte": _str(source_type, 20),
                    "obs": _str(notes, 4000),
                },
            )
            return int(row.scalar())

    def finish_execution(self, execution_id: int, status: str = "SUCCESS") -> None:
        from sqlalchemy import text

        status_clean = str(status).strip().upper()
        if status_clean not in {"SUCCESS", "ERROR", "RUNNING"}:
            status_clean = "ERROR"
        with self._get_engine().begin() as conn:
            conn.execute(
                text(
                    f"UPDATE {self._results_schema}.execucao "
                    "SET sta_execucao = :status, dat_finalizacao = CURRENT_TIMESTAMP "
                    "WHERE seq_execucao = :seq"
                ),
                {"status": status_clean, "seq": execution_id},
            )

    def save_quality_scores(self, execution_id: int, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return

        from sqlalchemy import text

        with self._get_engine().begin() as conn:
            dim_codes = self._load_dimension_codes(conn)
            rows = [
                {
                    "seq_exec": execution_id,
                    "cod_medida": _str(row.get("Component"), 10),
                    "cod_dim": dim_codes.get(_norm(row.get("Dimension"))),
                    "tip_res": _str(row.get("ScoreType"), 20),
                    "dsc_res": _str(row.get("Description"), 400),
                    "peso": _float(row.get("Weight")),
                    "valor": _float(row.get("Value")),
                }
                for _, row in df.iterrows()
            ]
            conn.execute(
                text(
                    f"INSERT INTO {self._results_schema}.resultado_qualidade "
                    "(seq_execucao, cod_medida, cod_dimensao, tip_resultado, "
                    " dsc_resultado, num_peso, num_valor) "
                    "VALUES (:seq_exec, :cod_medida, :cod_dim, :tip_res, "
                    "        :dsc_res, :peso, :valor)"
                ),
                rows,
            )
        print(f"[postgresql] resultado_qualidade: {len(rows)} rows inserted (execucao {execution_id})")

    def save_doc_code_issues(self, execution_id: int, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return

        from sqlalchemy import text

        with self._get_engine().begin() as conn:
            dim_codes = self._load_dimension_codes(conn)
            cod_conformity = dim_codes.get("conformity")
            rows = [
                {
                    "seq_exec": execution_id,
                    "cod_medida": _str(row.get("rule"), 10),
                    "cod_dim": cod_conformity,
                    "dsc_prob": _str(row.get("desc"), 200),
                    "owner": _str(row.get("owner"), 128),
                    "tabela": _str(row.get("table"), 128),
                    "coluna": _str(row.get("column"), 128),
                    "valor": _str(row.get("value"), 30),
                }
                for _, row in df.iterrows()
            ]
            conn.execute(
                text(
                    f"INSERT INTO {self._results_schema}.problema_execucao "
                    "(seq_execucao, cod_medida, cod_dimensao, dsc_problema, "
                    " dsc_owner, nom_tabela, nom_coluna, dsc_valor) "
                    "VALUES (:seq_exec, :cod_medida, :cod_dim, :dsc_prob, "
                    "        :owner, :tabela, :coluna, :valor)"
                ),
                rows,
            )
        print(f"[postgresql] problema_execucao: {len(rows)} rows inserted (execucao {execution_id})")

    def dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._dim_codes = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_engine(self):
        if self._engine is None:
            self._engine = build_database_engine(self.connection_settings)
        return self._engine

    def _load_dimension_codes(self, conn) -> dict[str, int]:
        if self._dim_codes is not None:
            return self._dim_codes
        from sqlalchemy import text

        rows = conn.execute(
            text(f"SELECT cod_dimensao, dsc_dimensao FROM {self._results_schema}.dimensao")
        ).fetchall()
        self._dim_codes = {str(r[1]).strip().lower(): int(r[0]) for r in rows}
        return self._dim_codes


# ------------------------------------------------------------------
# Private value helpers
# ------------------------------------------------------------------

def _str(value: object, max_len: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:max_len] if text else None


def _float(value: object) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
        return None if math.isnan(f) else f
    except (ValueError, TypeError):
        return None


def _norm(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()
