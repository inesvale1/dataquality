from __future__ import annotations

import math

import pandas as pd

from dataquality.infrastructure.io.secure_credentials import (
    DatabaseConnectionSettings,
    build_database_engine,
)

_SCHEMA = "DATA_QUALITY"


class OracleResultExporter:
    """Writes framework output to the DATA_QUALITY schema tables.

    Target tables (must exist before running):
      DATA_QUALITY.EXECUCAO
      DATA_QUALITY.RESULTADO_QUALIDADE
      DATA_QUALITY.PROBLEMA_EXECUCAO

    Credentials are never stored in config files: the password is read from
    the OS keyring via the same DatabaseConnectionSettings mechanism used for
    reading metadata.
    """

    def __init__(self, connection_settings: DatabaseConnectionSettings):
        self.connection_settings = connection_settings
        self._engine = None
        self._dim_codes: dict[str, int] | None = None  # cache: lower(DSC_DIMENSAO) → COD_DIMENSAO

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def begin_execution(
        self,
        owner: str,
        source_type: str = "ORACLE",
        notes: str | None = None,
    ) -> int:
        """Insert a row into EXECUCAO with status RUNNING and return SEQ_EXECUCAO."""
        from sqlalchemy import text

        with self._get_engine().begin() as conn:
            seq_id = conn.execute(
                text(f"SELECT {_SCHEMA}.SQ_EXECUCAO.NEXTVAL FROM DUAL")
            ).scalar()
            conn.execute(
                text(
                    f"INSERT INTO {_SCHEMA}.EXECUCAO "
                    "(SEQ_EXECUCAO, DSC_OWNER, TIP_FONTE, STA_EXECUCAO, DAT_EXECUCAO, DSC_OBSERVACAO) "
                    "VALUES (:seq, :owner, :fonte, 'RUNNING', SYSTIMESTAMP, :obs)"
                ),
                {
                    "seq": seq_id,
                    "owner": _str(owner, 128),
                    "fonte": _str(source_type, 20),
                    "obs": _str(notes, 4000),
                },
            )
        return int(seq_id)

    def finish_execution(self, execution_id: int, status: str = "SUCCESS") -> None:
        """Update EXECUCAO status and set DAT_FINALIZACAO."""
        from sqlalchemy import text

        status_clean = str(status).strip().upper()
        if status_clean not in {"SUCCESS", "ERROR", "RUNNING"}:
            status_clean = "ERROR"
        with self._get_engine().begin() as conn:
            conn.execute(
                text(
                    f"UPDATE {_SCHEMA}.EXECUCAO "
                    "SET STA_EXECUCAO = :status, DAT_FINALIZACAO = SYSTIMESTAMP "
                    "WHERE SEQ_EXECUCAO = :seq"
                ),
                {"status": status_clean, "seq": execution_id},
            )

    def save_quality_scores(self, execution_id: int, df: pd.DataFrame) -> None:
        """Bulk-insert QUALITY_SCORES rows into RESULTADO_QUALIDADE.

        Column mapping (DataFrame → Oracle):
          ScoreType  → TIP_RESULTADO
          Component  → COD_MEDIDA
          Dimension  → COD_DIMENSAO  (resolved via DIMENSAO domain table)
          Description→ DSC_RESULTADO
          Weight     → NUM_PESO
          Value      → NUM_VALOR
        """
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
                    f"INSERT INTO {_SCHEMA}.RESULTADO_QUALIDADE "
                    "(SEQ_RESULTADO_QUALIDADE, SEQ_EXECUCAO, COD_MEDIDA, COD_DIMENSAO, "
                    " TIP_RESULTADO, DSC_RESULTADO, NUM_PESO, NUM_VALOR) "
                    f"VALUES ({_SCHEMA}.SQ_RESULTADO_QUALIDADE.NEXTVAL, :seq_exec, :cod_medida, "
                    "        :cod_dim, :tip_res, :dsc_res, :peso, :valor)"
                ),
                rows,
            )
        print(f"[oracle] RESULTADO_QUALIDADE: {len(rows)} rows inserted (execucao {execution_id})")

    def save_doc_code_issues(self, execution_id: int, df: pd.DataFrame) -> None:
        """Bulk-insert CPF/CNPJ/CGF issues into PROBLEMA_EXECUCAO.

        Column mapping (DataFrame → Oracle):
          rule   → COD_MEDIDA
          desc   → DSC_PROBLEMA
          owner  → DSC_OWNER
          table  → NOM_TABELA
          column → NOM_COLUNA
          value  → DSC_VALOR
          (Conformity dimension resolved automatically via DIMENSAO table)
        """
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
                    f"INSERT INTO {_SCHEMA}.PROBLEMA_EXECUCAO "
                    "(SEQ_PROBLEMA_EXECUCAO, SEQ_EXECUCAO, COD_MEDIDA, COD_DIMENSAO, "
                    " DSC_PROBLEMA, DSC_OWNER, NOM_TABELA, NOM_COLUNA, DSC_VALOR) "
                    f"VALUES ({_SCHEMA}.SQ_PROBLEMA_EXECUCAO.NEXTVAL, :seq_exec, :cod_medida, "
                    "        :cod_dim, :dsc_prob, :owner, :tabela, :coluna, :valor)"
                ),
                rows,
            )
        print(f"[oracle] PROBLEMA_EXECUCAO: {len(rows)} rows inserted (execucao {execution_id})")

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
        """Return cached dict of lower(DSC_DIMENSAO) → COD_DIMENSAO from DIMENSAO table."""
        if self._dim_codes is not None:
            return self._dim_codes
        from sqlalchemy import text

        rows = conn.execute(
            text(f"SELECT COD_DIMENSAO, DSC_DIMENSAO FROM {_SCHEMA}.DIMENSAO")
        ).fetchall()
        self._dim_codes = {str(r[1]).strip().lower(): int(r[0]) for r in rows}
        return self._dim_codes


def build_oracle_exporter(
    connection_settings: DatabaseConnectionSettings,
) -> OracleResultExporter:
    return OracleResultExporter(connection_settings)


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
    """Normalize a dimension label for dict lookup."""
    if value is None:
        return ""
    return str(value).strip().lower()
