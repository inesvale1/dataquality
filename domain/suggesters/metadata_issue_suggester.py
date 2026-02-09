from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from dataquality.domain.config.validation_config import ValidationConfig


@dataclass
class LLMCommentSuggester:
    """
    Optional LLM-based suggester. Disabled by default.
    Implement suggest_comment to integrate a real model later.
    """

    enabled: bool = False

    def suggest_comment(self, context: Dict[str, Any]) -> Optional[str]:
        return None


class MetadataIssueSuggester:
    def __init__(
        self,
        db_type: str = "Oracle",
        config: Optional[ValidationConfig] = None,
        llm_comment_suggester: Optional[LLMCommentSuggester] = None,
    ):
        self.db_type = (db_type or "").strip()
        self.config = config or ValidationConfig()
        self.llm_comment_suggester = llm_comment_suggester or LLMCommentSuggester(enabled=False)

    def apply(self, issues_df: pd.DataFrame, schema_df: pd.DataFrame) -> pd.DataFrame:
        if issues_df is None or issues_df.empty:
            return self._ensure_columns(issues_df)

        schema_lookup = self._build_schema_lookup(schema_df)
        suggestions = []
        for _, row in issues_df.iterrows():
            suggestion = self._suggest_row(row, schema_lookup)
            suggestions.append(suggestion)

        df_suggestions = pd.DataFrame(suggestions)
        df_out = issues_df.copy()
        for col in df_suggestions.columns:
            df_out[col] = df_suggestions[col]
        return df_out

    def _ensure_columns(self, issues_df: pd.DataFrame) -> pd.DataFrame:
        df_out = issues_df.copy() if issues_df is not None else pd.DataFrame()
        for col in ("COLUMN_TYPE", "SUGGESTED_VALUE", "SUGGESTED_SOURCE", "SUGGESTED_CONFIDENCE", "SUGGESTED_DDL"):
            if col not in df_out.columns:
                df_out[col] = ""
        return df_out

    def _build_schema_lookup(self, schema_df: pd.DataFrame) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
        if schema_df is None or schema_df.empty:
            return {}
        required = {"OWNER", "TABLE_NAME", "COLUMN_NAME"}
        if not required.issubset(schema_df.columns):
            return {}
        lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for _, row in schema_df.iterrows():
            key = (str(row.get("OWNER", "")).upper(), str(row.get("TABLE_NAME", "")).upper(), str(row.get("COLUMN_NAME", "")).upper())
            if key not in lookup:
                lookup[key] = row.to_dict()
        return lookup

    def _suggest_row(self, row: pd.Series, schema_lookup: Dict[Tuple[str, str, str], Dict[str, Any]]) -> Dict[str, Any]:
        rule = self._clean_str(row.get("rule", "")).upper()
        owner = self._clean_str(row.get("owner", "")).upper()
        table = self._clean_str(row.get("table", "")).upper()
        column = self._clean_str(row.get("column", "")).upper()
        constraint_name = self._clean_str(row.get("constraint_name", "")).upper()

        schema_row = schema_lookup.get((owner, table, column), {})
        column_type = self._clean_str(row.get("data_type", "")).upper()
        if not column_type:
            column_type = self._clean_str(schema_row.get("DATA_TYPE", "")).upper()

        suggested_value = ""
        source = ""
        confidence = 0.0

        if rule == "MQME014":
            suggested_value, source, confidence = self._suggest_column_prefix(column, table, column_type)
        elif rule == "MQME008":
            suggested_value, source, confidence = self._suggest_comment(table, column, column_type)
        elif rule == "MQME012":
            suggested_value, source, confidence = self._suggest_singular_table(table)
        elif rule == "MQME013":
            suggested_value, source, confidence = self._suggest_shorter_name(table, self.config.max_table_len)
        elif rule == "MQME015":
            suggested_value, source, confidence = self._suggest_shorter_name(column, self.config.max_column_len)
        elif rule == "MQME009":
            suggested_value, source, confidence = self._suggest_constraint_name("PK_", table, column, constraint_name)
        elif rule == "MQME010":
            suggested_value, source, confidence = self._suggest_constraint_name("FK_", table, column, constraint_name)
        elif rule == "MQME011":
            suggested_value, source, confidence = self._suggest_unique_name(table, column, constraint_name)
        elif rule in ("MQME020", "MQME021"):
            suggested_value, source, confidence = "0", "RULES", 0.6

        ddl = self._build_ddl(rule, owner, table, column, constraint_name, suggested_value)

        return {
            "COLUMN_TYPE": column_type,
            "SUGGESTED_VALUE": suggested_value,
            "SUGGESTED_SOURCE": source,
            "SUGGESTED_CONFIDENCE": confidence,
            "SUGGESTED_DDL": ddl,
        }

    def _suggest_column_prefix(self, column: str, table: str, column_type: str) -> Tuple[str, str, float]:
        if not column:
            return "", "", 0.0
        if column.upper() == "DATA" and table:
            return f"DAT_{table}", "RULES", 0.9
        base = self._strip_prefix(column)
        prefix = self._choose_prefix(column_type, base)
        if not prefix:
            return "", "", 0.0
        suggested = prefix + base
        return suggested, "RULES", 0.85

    def _suggest_comment(self, table: str, column: str, column_type: str) -> Tuple[str, str, float]:
        if not column:
            return "", "", 0.0
        prefix = self._extract_prefix(column)
        base = self._strip_prefix(column)
        tokens = [t for t in base.split("_") if t]
        core = " ".join([t.lower() for t in tokens]) if tokens else column.lower()
        if column_type in {"DATE", "DATETIME", "TIMESTAMP"}:
            context = self._table_context(table)
            comment = f"Data de {context}."
        else:
            comment = self._comment_by_prefix(prefix, core)
        confidence = 0.8 if tokens else 0.5

        if self.llm_comment_suggester.enabled and confidence < 0.75:
            context = {"table": table, "column": column, "data_type": schema_row.get("DATA_TYPE", "")}
            llm_comment = self.llm_comment_suggester.suggest_comment(context)
            if llm_comment:
                return llm_comment, "LLM", 0.7

        return comment, "RULES", confidence

    def _suggest_singular_table(self, table: str) -> Tuple[str, str, float]:
        if not table:
            return "", "", 0.0
        if table.endswith("ES"):
            return table[:-2], "RULES", 0.7
        if table.endswith("S"):
            return table[:-1], "RULES", 0.7
        return "", "", 0.0

    def _suggest_shorter_name(self, name: str, limit: int) -> Tuple[str, str, float]:
        if not name or len(name) <= limit:
            return "", "", 0.0
        shortened = self._abbreviate(name)
        if len(shortened) > limit:
            shortened = shortened[:limit]
        return shortened, "RULES", 0.6

    def _suggest_constraint_name(self, prefix: str, table: str, column: str, current: str) -> Tuple[str, str, float]:
        if not table:
            return "", "", 0.0
        suggested = f"{prefix}{table}" if not column else f"{prefix}{table}_{column}"
        return suggested, "RULES", 0.8

    def _suggest_unique_name(self, table: str, column: str, current: str) -> Tuple[str, str, float]:
        if not table:
            return "", "", 0.0
        suggested = f"{table}_{column}_UK" if column else f"{table}_UK"
        return suggested, "RULES", 0.8

    def _build_ddl(
        self,
        rule: str,
        owner: str,
        table: str,
        column: str,
        constraint_name: str,
        suggested_value: str,
    ) -> str:
        if not suggested_value:
            return ""
        if self.db_type.lower() != "oracle":
            return ""

        qualified_table = self._qualify_table(owner, table)

        if rule in ("MQME014", "MQME015"):
            return f"ALTER TABLE {qualified_table} RENAME COLUMN {column} TO {suggested_value};"
        if rule in ("MQME012", "MQME013"):
            return f"ALTER TABLE {qualified_table} RENAME TO {suggested_value};"
        if rule == "MQME008":
            comment = suggested_value.replace("'", "''")
            return f"COMMENT ON COLUMN {qualified_table}.{column} IS '{comment}';"
        if rule in ("MQME009", "MQME010", "MQME011"):
            if not constraint_name:
                return ""
            return f"ALTER TABLE {qualified_table} RENAME CONSTRAINT {constraint_name} TO {suggested_value};"

        return ""

    def _qualify_table(self, owner: str, table: str) -> str:
        if owner:
            return f"{owner}.{table}"
        return table

    def _clean_str(self, value: Any) -> str:
        s = str(value).strip()
        if s.lower() in {"", "nan", "none", "<na>", "null"}:
            return ""
        return s

    def _strip_prefix(self, name: str) -> str:
        if not name:
            return name
        parts = name.split("_", 1)
        if len(parts) == 2 and len(parts[0]) == 3:
            return parts[1]
        return name

    def _extract_prefix(self, name: str) -> str:
        parts = name.split("_", 1)
        if len(parts) == 2 and len(parts[0]) == 3:
            return parts[0] + "_"
        return ""

    def _choose_prefix(self, data_type: str, base: str) -> str:
        data_type = data_type.upper()
        base_upper = base.upper()

        data_type_map = {
            "NUMBER": "NUM_",
            "DATE": "DAT_",
            "TIMESTAMP": "DAT_",
            "CHAR": "DSC_",
            "NCHAR": "DSC_",
            "VARCHAR2": "DSC_",
            "NVARCHAR2": "DSC_",
            "CLOB": "TXT_",
            "RAW": "BIN_",
            "BLOB": "BIN_",
        }
        if data_type in data_type_map:
            return data_type_map[data_type]

        hint_map = [
            ("COD", "COD_"),
            ("ID", "COD_"),
            ("NOM", "NOM_"),
            ("NOME", "NOM_"),
            ("QTD", "QTD_"),
            ("QTDE", "QTD_"),
            ("QUANT", "QTD_"),
            ("DATA", "DAT_"),
            ("DT", "DAT_"),
            ("HORA", "HOR_"),
            ("HR", "HOR_"),
            ("SIT", "SIT_"),
            ("STATUS", "STA_"),
            ("DESC", "DSC_"),
            ("DESCR", "DSC_"),
            ("TXT", "TXT_"),
            ("VALOR", "VLR_"),
            ("VL", "VLR_"),
            ("TOTAL", "TOT_"),
            ("TIPO", "TIP_"),
            ("SEQ", "SEQ_"),
        ]
        for token, prefix in hint_map:
            if token in base_upper:
                return prefix

        return "TXT_"

    def _comment_by_prefix(self, prefix: str, core: str) -> str:
        if prefix == "COD_":
            return f"Codigo de {core}."
        if prefix == "NOM_":
            return f"Nome de {core}."
        if prefix == "DAT_":
            return f"Data de {core}."
        if prefix == "HOR_":
            return f"Hora de {core}."
        if prefix == "QTD_":
            return f"Quantidade de {core}."
        if prefix == "NUM_":
            return f"Numero de {core}."
        if prefix == "VLR_":
            return f"Valor de {core}."
        if prefix == "TOT_":
            return f"Total de {core}."
        if prefix == "SIT_":
            return f"Situacao de {core}."
        if prefix == "STA_":
            return f"Status de {core}."
        if prefix == "TIP_":
            return f"Tipo de {core}."
        if prefix == "SEQ_":
            return f"Sequencia de {core}."
        if prefix == "BIN_":
            return f"Conteudo binario de {core}."
        if prefix == "TXT_":
            return f"Texto de {core}."
        if prefix == "DSC_":
            return f"Descricao de {core}."
        return f"Descricao de {core}."

    def _abbreviate(self, name: str) -> str:
        abbreviations = {
            "DATA": "DAT",
            "INFORMACAO": "INF",
            "INFORMACOES": "INF",
            "QUANTIDADE": "QTD",
            "NUMERO": "NUM",
            "DESCRICAO": "DSC",
            "CATEGORIA": "CAT",
            "REFERENCIA": "REF",
            "DOCUMENTO": "DOC",
            "PROCESSO": "PRO",
            "CODIGO": "COD",
            "HISTORICO": "HIS",
            "PERCENTUAL": "PER",
            "SITUACAO": "SIT",
            "STATUS": "STA",
        }
        parts = name.upper().split("_")
        new_parts = [abbreviations.get(p, p) for p in parts if p]
        return "_".join(new_parts)

    def _table_context(self, table: str) -> str:
        if not table:
            return "evento"
        parts = [p.lower() for p in table.split("_") if p]
        return " ".join(parts) if parts else "evento"
