from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import re

from dataquality.domain.config.data_quality_semantic_config import SEMANTIC_FORMAT_RULES, SemanticFormatRuleSpec
from dataquality.domain.config.validation_config import NamedLengthRule, ValidationConfig
from dataquality.domain.validators.rules import Rule, apply_rules


class MetadataValidator:
    """
    Validates metadata completeness, consistency, and naming standards for schema dictionaries.
    """

    LENGTH_REQUIRED_TYPES = {
        "CHAR",
        "NCHAR",
        "VARCHAR2",
        "NVARCHAR2",
        "RAW",
    }
    FORMAT_CONFORMITY_TEXT_TYPES = {
        "CHAR",
        "NCHAR",
        "VARCHAR",
        "VARCHAR2",
        "NVARCHAR",
        "NVARCHAR2",
        "CLOB",
        "TEXT",
        "LONG",
    }
    FORMAT_CONFORMITY_DATE_TYPES = {
        "DATE",
        "DATETIME",
        "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP WITH LOCAL TIME ZONE",
    }
    FORMAT_CONFORMITY_COLUMNS = (
        "FORMAT_CONFORMITY_CANDIDATE",
        "FORMAT_CONFORMITY_METRIC",
        "FORMAT_CONFORMITY_DIMENSION",
        "FORMAT_CONFORMITY_SEMANTIC_TAG",
        "FORMAT_CONFORMITY_RULE_TYPE",
        "FORMAT_CONFORMITY_EXPECTED_FORMAT",
        "FORMAT_CONFORMITY_PRIORITY",
        "FORMAT_CONFORMITY_DESCRIPTION",
    )
    REDUNDANCY_COLUMNS = (
        "REDUNDANCY_CANDIDATE",
        "REDUNDANCY_METRIC",
        "REDUNDANCY_DIMENSION",
        "REDUNDANCY_RULE_TYPE",
        "REDUNDANCY_PRIORITY",
        "REDUNDANCY_DESCRIPTION",
        "REDUNDANCY_CALCULATION_METHOD",
    )
    REDUNDANCY_TEXT_TYPES = FORMAT_CONFORMITY_TEXT_TYPES | {"XMLTYPE"}
    TYPE_NAMING_TEXT_TYPES = FORMAT_CONFORMITY_TEXT_TYPES | {"RAW"}
    TYPE_NAMING_NUMERIC_TYPES = {
        "NUMBER",
        "INTEGER",
        "INT",
        "SMALLINT",
        "BIGINT",
        "FLOAT",
        "DECIMAL",
        "NUMERIC",
        "BINARY_FLOAT",
        "BINARY_DOUBLE",
    }
    TYPE_NAMING_LARGE_TEXT_TYPES = {"CLOB", "TEXT", "LONG"}
    REDUNDANCY_POSITIVE_NAME_TOKENS = {
        "NOM",
        "NOME",
        "DSC",
        "DESC",
        "DESCR",
        "TXT",
        "TEXTO",
        "END",
        "ENDERECO",
        "LOGRADOURO",
        "BAIRRO",
        "CIDADE",
        "MUNICIPIO",
        "UF",
        "EMAIL",
        "E_MAIL",
        "IP",
        "FONE",
        "TELEFONE",
        "CEL",
        "CELULAR",
    }
    REDUNDANCY_NEGATIVE_NAME_TOKENS = {
        "COD",
        "ID",
        "CPF",
        "CNPJ",
        "CEP",
        "DAT",
        "DATA",
        "DT",
        "NUM",
        "SEQ",
        "QTD",
        "TOT",
        "VLR",
        "SIT",
        "STA",
        "TIP",
        "PK",
        "FK",
        "UK",
    }

    def __init__(self, df: pd.DataFrame, table_plural_exceptions: List[str], config: ValidationConfig | None = None):
        self.df = df.copy()
        self.cfg = config or ValidationConfig()
        self.table_plural_exceptions = table_plural_exceptions

        required = {
            "OWNER",
            "TABLE_NAME",
            "NUM_ROWS",
            "COLUMN_NAME",
            "DATA_TYPE",
            "DATA_LENGTH",
            "NULLABLE",
            "DEFAULT_ON_NULL",
            "CONSTRAINTS",
            "IS_PK",
            "IS_FK",
            "IS_UNIQUE",
        }
        missing = [c for c in required if c not in self.df.columns]
        if missing:
            raise ValueError(f"Input DataFrame is missing required columns: {missing}")

        for col in ("OWNER", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"):
            self.df[col] = self.df[col].astype(str).str.strip()
        if "COMMENTS" in self.df.columns:
            self.df["COMMENTS"] = self.df["COMMENTS"].astype(str)

        self.number_tables = self.df["TABLE_NAME"].nunique()
        self.number_columns = self.df.shape[0]
        self.number_pks = self.df["IS_PK"].sum()
        self.number_fks = self.df["IS_FK"].sum()
        self.number_uks = self.df["IS_UNIQUE"].sum()

        self.list_tab_plural: List[Dict[str, Any]] = []
        self.list_tab_name_too_long: List[Dict[str, Any]] = []
        self.list_col_pref_no_standard: List[Dict[str, Any]] = []
        self.list_col_name_too_long: List[Dict[str, Any]] = []
        self.list_col_comment_missing: List[Dict[str, Any]] = []
        self.list_pk_bad_prefix: List[Dict[str, Any]] = []
        self.list_fk_bad_prefix: List[Dict[str, Any]] = []
        self.list_unique_bad_suffix: List[Dict[str, Any]] = []
        self.list_table_without_pk: List[Dict[str, Any]] = []
        self.list_table_without_integrity_constraint: List[Dict[str, Any]] = []
        self.list_identifier_not_protected: List[Dict[str, Any]] = []
        self.list_type_naming_mismatch: List[Dict[str, Any]] = []

        self.number_tables_without_pk = 0
        self.number_tables_without_pk_or_uk = 0
        self.number_identifier_like_columns = 0
        self.number_identifier_like_columns_without_protection = 0
        self.number_type_naming_candidates = 0
        self.number_type_naming_noncompliant_columns = 0

        self.issues_df: pd.DataFrame | None = None

    # ---------------- accessory methods ----------------
    def get_number_tables(self) -> int:
        return int(self.number_tables)

    def get_number_columns(self) -> int:
        return int(self.number_columns)

    def get_number_primary_keys(self) -> int:
        return int(self.number_pks)

    def get_number_foreign_keys(self) -> int:
        return int(self.number_fks)

    def get_number_unique_keys(self) -> int:
        return int(self.number_uks)

    def get_number_length_required(self) -> int:
        return int(self._is_length_required().sum())

    def get_number_number_types(self) -> int:
        return int(self.df["DATA_TYPE"].str.upper().eq("NUMBER").sum())

    def get_number_tables_without_pk(self) -> int:
        return int(self.number_tables_without_pk)

    def get_number_tables_without_pk_or_uk(self) -> int:
        return int(self.number_tables_without_pk_or_uk)

    def get_number_identifier_like_columns(self) -> int:
        return int(self.number_identifier_like_columns)

    def get_number_identifier_like_columns_without_protection(self) -> int:
        return int(self.number_identifier_like_columns_without_protection)

    def get_number_type_naming_candidates(self) -> int:
        return int(self.number_type_naming_candidates)

    def get_number_type_naming_noncompliant_columns(self) -> int:
        return int(self.number_type_naming_noncompliant_columns)

    def get_rows_by_table(self) -> pd.Series:
        col = self._get_rows_column()
        if not col:
            return pd.Series(dtype=int)

        values = pd.to_numeric(self.df[col], errors="coerce")
        if values.isna().all():
            cleaned = (
                self.df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", "", regex=False)
            )
            values = pd.to_numeric(cleaned, errors="coerce")

        rows_by_table = values.groupby(self.df["TABLE_NAME"]).max().fillna(0).astype(int)
        return rows_by_table

    def get_total_rows_schema(self) -> int:
        rows_by_table = self.get_rows_by_table()
        if rows_by_table.empty:
            return 0
        return int(rows_by_table.sum())

    def get_total_cells_schema(self) -> int:
        rows_by_table = self.get_rows_by_table()
        if rows_by_table.empty:
            return 0
        cols_by_table = self.df.groupby("TABLE_NAME")["COLUMN_NAME"].nunique()
        aligned = rows_by_table.reindex(cols_by_table.index).fillna(0).astype(int)
        total_cells = (aligned * cols_by_table).sum()
        return int(total_cells)

    def get_num_nulls_nullable_without_default(self) -> int:
        if "NUM_NULLS" not in self.df.columns:
            return 0
        nullable = self.df["NULLABLE"].astype(bool)
        default_on_null = self.df["DEFAULT_ON_NULL"].astype(bool)
        mask = nullable & (~default_on_null)
        num_nulls = pd.to_numeric(self.df["NUM_NULLS"], errors="coerce").fillna(0)
        return int(num_nulls[mask].sum())

    def get_num_nulls_by_table_nullable_without_default(self) -> pd.Series:
        if "NUM_NULLS" not in self.df.columns:
            return pd.Series(dtype=int)
        nullable = self.df["NULLABLE"].astype(bool)
        default_on_null = self.df["DEFAULT_ON_NULL"].astype(bool)
        mask = nullable & (~default_on_null)
        num_nulls = pd.to_numeric(self.df["NUM_NULLS"], errors="coerce").fillna(0)
        return (
            num_nulls[mask]
            .groupby(self.df.loc[mask, "TABLE_NAME"])
            .sum()
            .fillna(0)
            .astype(int)
        )

    def get_null_percent_by_table_nullable_without_default(self) -> pd.Series:
        num_nulls_by_table = self.get_num_nulls_by_table_nullable_without_default()
        rows_by_table = self.get_rows_by_table()
        if num_nulls_by_table.empty or rows_by_table.empty:
            return pd.Series(dtype=float)

        cols_by_table = self.df.groupby("TABLE_NAME")["COLUMN_NAME"].nunique()
        total_cells_by_table = (rows_by_table.reindex(cols_by_table.index).fillna(0).astype(int) * cols_by_table)
        aligned_nulls = num_nulls_by_table.reindex(total_cells_by_table.index, fill_value=0).astype(float)
        denominator = total_cells_by_table.astype(float).replace(0, float("nan"))
        null_percent = (aligned_nulls / denominator).mul(100).fillna(0.0)
        return null_percent

    def annotate_null_percentages(self, schema_df: pd.DataFrame | None = None) -> pd.DataFrame:
        df_out = (schema_df.copy() if schema_df is not None else self.df.copy())
        if "NULL_PERCENT" not in df_out.columns:
            df_out["NULL_PERCENT"] = ""

        if df_out.empty or "NUM_ROWS" not in df_out.columns or "NUM_NULLS" not in df_out.columns:
            return df_out

        num_rows = pd.to_numeric(df_out["NUM_ROWS"], errors="coerce").astype(float)
        num_nulls = pd.to_numeric(df_out["NUM_NULLS"], errors="coerce").astype(float)
        denominator = num_rows.replace(0, float("nan"))
        null_percent = (num_nulls / denominator).mul(100).fillna(0.0)
        df_out["NULL_PERCENT"] = null_percent.map(lambda value: f"{value:.2f}%")
        return df_out

    def annotate_data_quality_candidates(self, schema_df: pd.DataFrame | None = None) -> pd.DataFrame:
        df_out = self.annotate_null_percentages(schema_df)
        for col in self.FORMAT_CONFORMITY_COLUMNS:
            if col not in df_out.columns:
                df_out[col] = ""
        for col in self.REDUNDANCY_COLUMNS:
            if col not in df_out.columns:
                df_out[col] = ""

        if df_out.empty or "COLUMN_NAME" not in df_out.columns or "DATA_TYPE" not in df_out.columns:
            return df_out

        df_out["FORMAT_CONFORMITY_CANDIDATE"] = False
        df_out["REDUNDANCY_CANDIDATE"] = False

        for idx, row in df_out.iterrows():
            spec = self._match_format_conformity_rule(
                column_name=str(row.get("COLUMN_NAME", "")),
                data_type=str(row.get("DATA_TYPE", "")),
            )
            if spec is not None:
                df_out.at[idx, "FORMAT_CONFORMITY_CANDIDATE"] = True
                df_out.at[idx, "FORMAT_CONFORMITY_METRIC"] = spec.metric
                df_out.at[idx, "FORMAT_CONFORMITY_DIMENSION"] = spec.dimension
                df_out.at[idx, "FORMAT_CONFORMITY_SEMANTIC_TAG"] = spec.semantic_key
                df_out.at[idx, "FORMAT_CONFORMITY_RULE_TYPE"] = spec.rule_type
                df_out.at[idx, "FORMAT_CONFORMITY_EXPECTED_FORMAT"] = spec.expected_format
                df_out.at[idx, "FORMAT_CONFORMITY_PRIORITY"] = spec.priority
                df_out.at[idx, "FORMAT_CONFORMITY_DESCRIPTION"] = spec.description

            if self._is_redundancy_candidate(row):
                df_out.at[idx, "REDUNDANCY_CANDIDATE"] = True
                df_out.at[idx, "REDUNDANCY_METRIC"] = "Redundancy detection"
                df_out.at[idx, "REDUNDANCY_DIMENSION"] = "Uniqueness"
                df_out.at[idx, "REDUNDANCY_RULE_TYPE"] = "duplicate_ratio"
                df_out.at[idx, "REDUNDANCY_PRIORITY"] = "medium"
                df_out.at[idx, "REDUNDANCY_CALCULATION_METHOD"] = (
                    "METADATA_STATISTICS" if self._has_distinct_statistics(row) else "DATA_SCAN_REQUIRED"
                )
                df_out.at[idx, "REDUNDANCY_DESCRIPTION"] = (
                    "Coluna nao-chave com conteudo descritivo, candidata a deteccao de duplicatas logicas."
                )

        return df_out

    def annotate_format_conformity_candidates(self, schema_df: pd.DataFrame | None = None) -> pd.DataFrame:
        return self.annotate_data_quality_candidates(schema_df)

    # ----------------------------- public API -----------------------------
    def run_all(self) -> pd.DataFrame:
        self._validate_tables()
        self._validate_columns()
        self._validate_constraints()
        self._validate_constraint_coverage()
        self._validate_identifier_protection()
        self._validate_type_naming_compliance()
        metadata_rules = self._build_rules()
        df_metadata = apply_rules(self.df, metadata_rules)
        self.issues_df = self._combine_issues(df_metadata)
        return self.issues_df

    # ------------------------- validation helpers ------------------------
    def _missing_text(self, s: pd.Series) -> pd.Series:
        s_str = (
            s.astype("string")
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )
        null_tokens = {"", "nan", "<na>", "none", "null", "n/a", "na"}
        return s_str.isna() | s_str.str.lower().isin(null_tokens)

    def _is_length_required(self) -> pd.Series:
        return self.df["DATA_TYPE"].str.upper().isin(self.LENGTH_REQUIRED_TYPES)

    def _get_rows_column(self) -> str | None:
        for candidate in ("NUM_ROWS", "TABLE_ROWS", "ROW_COUNT"):
            if candidate in self.df.columns:
                return candidate
        return None

    def _coerce_bool_value(self, value: Any) -> bool:
        if pd.isna(value):
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        text = str(value).strip().lower()
        if text in {"", "0", "false", "f", "n", "no", "nao", "não"}:
            return False
        if text in {"1", "true", "t", "y", "yes", "s", "sim"}:
            return True
        return bool(text)

    def _to_number(self, value: Any) -> float | None:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric):
            return None
        return float(numeric)

    def _is_numeric_type(self, data_type: str) -> bool:
        return str(data_type).strip().upper() in self.TYPE_NAMING_NUMERIC_TYPES

    def _is_text_type(self, data_type: str) -> bool:
        return str(data_type).strip().upper() in self.TYPE_NAMING_TEXT_TYPES

    def _is_date_type(self, data_type: str) -> bool:
        return str(data_type).strip().upper() in self.FORMAT_CONFORMITY_DATE_TYPES

    def _is_indicator_compatible_type(self, data_type: str, data_length: float | None, data_scale: float | None) -> bool:
        normalized_type = str(data_type).strip().upper()
        indicator_text_lengths = {float(value) for value in self.cfg.type_naming.indicator_text_lengths}
        indicator_numeric_lengths = {float(value) for value in self.cfg.type_naming.indicator_numeric_lengths}
        indicator_numeric_scales = {float(value) for value in self.cfg.type_naming.indicator_numeric_scales}
        if normalized_type in self.TYPE_NAMING_NUMERIC_TYPES:
            return (
                data_scale in indicator_numeric_scales
                and (data_length is None or data_length in indicator_numeric_lengths)
            )
        if normalized_type in self.TYPE_NAMING_TEXT_TYPES:
            return data_length is None or data_length in indicator_text_lengths
        return False

    def _matches_identifier_profile(self, row: pd.Series) -> bool:
        normalized_name = self._normalize_semantic_name(str(row.get("COLUMN_NAME", "")))
        if not normalized_name:
            return False
        return any(
            re.search(pattern, normalized_name, flags=re.IGNORECASE)
            for pattern in self.cfg.type_naming.identifier_name_patterns
        )

    def _build_issue_entry(
        self,
        rule: str,
        desc: str,
        row: pd.Series | None = None,
        *,
        owner: str = "",
        table: str = "",
        column: str = "",
        constraint_name: str = "",
        length: Any = "",
        limit: Any = "",
        data_type: str = "",
    ) -> Dict[str, Any]:
        owner_value = owner or (row.get("OWNER", "") if row is not None else "")
        table_value = table or (row.get("TABLE_NAME", "") if row is not None else "")
        column_value = column or (row.get("COLUMN_NAME", "") if row is not None else "")
        data_type_value = data_type or (row.get("DATA_TYPE", "") if row is not None else "")
        return {
            "rule": rule,
            "desc": desc,
            "owner": owner_value,
            "table": table_value,
            "column": column_value,
            "constraint_name": constraint_name,
            "length": length,
            "limit": limit,
            "data_type": data_type_value,
        }

    def _validate_tables(self) -> None:
        plural_mask = self.df["TABLE_NAME"].str.upper().str.endswith("S")
        for _, row in self.df[plural_mask].drop_duplicates(subset=["OWNER", "TABLE_NAME"]).iterrows():
            table_name = row["TABLE_NAME"].upper()
            if table_name in [t.upper() for t in self.table_plural_exceptions]:
                continue
            self.list_tab_plural.append(
                {
                    "rule": "MQME012",
                    "desc": "Tables with plural names",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "data_type": row.get("DATA_TYPE", ""),
                }
            )

        too_long_mask = self.df["TABLE_NAME"].str.len() > self.cfg.max_table_len
        for _, row in self.df[too_long_mask].drop_duplicates(subset=["OWNER", "TABLE_NAME"]).iterrows():
            self.list_tab_name_too_long.append(
                {
                    "rule": "MQME013",
                    "desc": "Tables with names longer than recommended",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "length": int(len(row["TABLE_NAME"])),
                    "limit": self.cfg.max_table_len,
                    "data_type": "",
                }
            )

    def _validate_columns(self) -> None:
        allowed = tuple(self.cfg.prefix_names)
        bad_prefix = ~self.df["COLUMN_NAME"].str.upper().str.startswith(allowed)
        for _, row in self.df[bad_prefix].iterrows():
            self.list_col_pref_no_standard.append(
                {
                    "rule": "MQME014",
                    "desc": "Columns with non-standard prefixes",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "column": row["COLUMN_NAME"],
                    "data_type": row.get("DATA_TYPE", ""),
                }
            )

        too_long = self.df["COLUMN_NAME"].str.len() > self.cfg.max_column_len
        for _, row in self.df[too_long].iterrows():
            self.list_col_name_too_long.append(
                {
                    "rule": "MQME015",
                    "desc": "Columns with names longer than recommended",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "column": row["COLUMN_NAME"],
                    "length": int(len(row["COLUMN_NAME"])),
                    "limit": self.cfg.max_column_len,
                    "data_type": row.get("DATA_TYPE", ""),
                }
            )

        missing_comment = self._missing_text(self.df["COMMENTS"])
        for _, row in self.df[missing_comment].iterrows():
            self.list_col_comment_missing.append(
                {
                    "rule": "MQME008",
                    "desc": "Columns without comments",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "column": row["COLUMN_NAME"],
                    "data_type": row.get("DATA_TYPE", ""),
                }
            )

    def _parse_constraints(self, constraints_cell: Any) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        if not isinstance(constraints_cell, str):
            return result
        text = constraints_cell.strip()
        if not text:
            return result
        parts = [p.strip() for p in text.split(";") if p.strip()]
        pattern = re.compile(r"^\s*([^\s(]+)\s*\(([^)]*)\)\s*$", re.IGNORECASE)
        for part in parts:
            m = pattern.match(part)
            if not m:
                continue
            name = m.group(1).strip()
            details = m.group(2).strip()
            if not name:
                continue
            details_upper = details.upper()
            ctype = None
            for candidate in ("PRIMARY KEY", "FOREIGN KEY", "UNIQUE"):
                if candidate in details_upper:
                    ctype = candidate
                    break
            enabled = ("ENABLED" in details_upper) and ("DISABLED" not in details_upper)
            result.append({"name": name, "type": ctype, "enabled": enabled})
        return result

    def _validate_constraints(self) -> None:
        unique_pat = re.compile(self.cfg.unique_suffix_regex, flags=re.IGNORECASE)

        df = self.df.copy()
        for flag in ["IS_PK", "IS_FK", "IS_UNIQUE"]:
            df[flag] = df[flag].fillna(False).astype(bool)

        mask_has_constraints = df[["IS_PK", "IS_FK", "IS_UNIQUE"]].any(axis=1)
        df_to_check = df[mask_has_constraints]

        for _, row in df_to_check.iterrows():
            owner = row["OWNER"]
            table = row["TABLE_NAME"]
            column = row["COLUMN_NAME"]

            raw_constraints = row.get("CONSTRAINTS", "") or ""
            items = self._parse_constraints(raw_constraints)
            enabled = [c for c in items if c.get("enabled") is True]
            if not enabled:
                continue

            if row["IS_PK"]:
                pk_names = [c["name"] for c in enabled if c.get("type") == "PRIMARY KEY"]
                names_to_check = pk_names or [c["name"] for c in enabled]
                if not any(n.upper().startswith(self.cfg.pk_prefix.upper()) for n in names_to_check):
                    offender = names_to_check[0] if names_to_check else ""
                    self.list_pk_bad_prefix.append(
                        {
                    "rule": "MQME009",
                            "desc": "Total number of tables with non-standard primary key prefixes",
                            "owner": owner,
                            "table": table,
                            "column": column,
                            "constraint_name": offender,
                            "data_type": row.get("DATA_TYPE", ""),
                        }
                    )

            if row["IS_FK"]:
                fk_names = [c["name"] for c in enabled if c.get("type") == "FOREIGN KEY"]
                names_to_check = fk_names or [c["name"] for c in enabled]
                if not any(n.upper().startswith(self.cfg.fk_prefix.upper()) for n in names_to_check):
                    offender = names_to_check[0] if names_to_check else ""
                    self.list_fk_bad_prefix.append(
                        {
                    "rule": "MQME010",
                            "desc": "Total number of tables with non-standard foreign key prefixes",
                            "owner": owner,
                            "table": table,
                            "column": column,
                            "constraint_name": offender,
                            "data_type": row.get("DATA_TYPE", ""),
                        }
                    )

            if row["IS_UNIQUE"]:
                uq_names = [c["name"] for c in enabled if c.get("type") == "UNIQUE"]
                names_to_check = uq_names or [c["name"] for c in enabled]
                if not any(unique_pat.match(n) for n in names_to_check):
                    offender = names_to_check[0] if names_to_check else ""
                    self.list_unique_bad_suffix.append(
                        {
                    "rule": "MQME011",
                            "desc": "Total number of tables with non-standard unique key prefixes",
                            "owner": owner,
                            "table": table,
                            "column": column,
                            "constraint_name": offender,
                            "data_type": row.get("DATA_TYPE", ""),
                        }
                    )

    def _validate_constraint_coverage(self) -> None:
        df = self.df.copy()
        for flag in ["IS_PK", "IS_UNIQUE"]:
            df[flag] = df[flag].apply(self._coerce_bool_value)

        grouped = (
            df.groupby(["OWNER", "TABLE_NAME"], dropna=False)
            .agg(HAS_PK=("IS_PK", "any"), HAS_UK=("IS_UNIQUE", "any"))
            .reset_index()
        )

        self.number_tables_without_pk = 0
        self.number_tables_without_pk_or_uk = 0

        for _, row in grouped.iterrows():
            if not bool(row["HAS_PK"]):
                self.number_tables_without_pk += 1
                self.list_table_without_pk.append(
                    self._build_issue_entry(
                        "MQRL011",
                        "Table without primary key coverage",
                        owner=str(row["OWNER"]),
                        table=str(row["TABLE_NAME"]),
                    )
                )

            if not (bool(row["HAS_PK"]) or bool(row["HAS_UK"])):
                self.number_tables_without_pk_or_uk += 1
                self.list_table_without_integrity_constraint.append(
                    self._build_issue_entry(
                        "MQRL012",
                        "Table without PK or UK integrity constraint",
                        owner=str(row["OWNER"]),
                        table=str(row["TABLE_NAME"]),
                    )
                )

    def _validate_identifier_protection(self) -> None:
        protected_count = 0
        candidate_count = 0

        for _, row in self.df.iterrows():
            if not self._matches_identifier_profile(row):
                continue

            candidate_count += 1
            is_protected = any(
                self._coerce_bool_value(row.get(flag, False))
                for flag in ("IS_PK", "IS_FK", "IS_UNIQUE")
            )
            if is_protected:
                protected_count += 1
                continue

            self.list_identifier_not_protected.append(
                self._build_issue_entry(
                    "MQRL013",
                    "Identifier-like column is not protected by PK, FK, or UK",
                    row,
                )
            )

        self.number_identifier_like_columns = candidate_count
        self.number_identifier_like_columns_without_protection = max(candidate_count - protected_count, 0)

    def _validate_type_naming_compliance(self) -> None:
        candidate_indexes: set[Any] = set()
        offending_indexes: set[Any] = set()

        for idx, row in self.df.iterrows():
            issues = self._get_type_naming_issues(row)
            if not issues:
                continue

            candidate_indexes.add(idx)
            if any(issue["mismatch"] for issue in issues):
                offending_indexes.add(idx)
                for issue in issues:
                    if issue["mismatch"]:
                        self.list_type_naming_mismatch.append(
                            self._build_issue_entry(
                                "MQRL014",
                                issue["desc"],
                                row,
                                length=issue.get("length", ""),
                                limit=issue.get("limit", ""),
                            )
                        )
                continue

            candidate_indexes.add(idx)

        self.number_type_naming_candidates = len(candidate_indexes)
        self.number_type_naming_noncompliant_columns = len(offending_indexes)

    def _get_type_naming_issues(self, row: pd.Series) -> List[Dict[str, Any]]:
        normalized_name = self._normalize_semantic_name(str(row.get("COLUMN_NAME", "")))
        if not normalized_name:
            return []

        data_type = str(row.get("DATA_TYPE", "")).strip().upper()
        data_length = self._to_number(row.get("DATA_LENGTH", None))
        data_scale = self._to_number(row.get("DATA_SCALE", None))
        issues: List[Dict[str, Any]] = []

        def add_issue(desc: str, *, mismatch: bool, limit: Any = "", length: Any = "") -> None:
            issues.append({"desc": desc, "mismatch": mismatch, "limit": limit, "length": length})

        if self._matches_any_prefix(normalized_name, self.cfg.type_naming.date_prefixes):
            add_issue(
                "DAT_ column should use DATE or TIMESTAMP-compatible type",
                mismatch=not self._is_date_type(data_type),
                limit="DATE/TIMESTAMP",
                length=data_type,
            )

        if self._matches_any_prefix(normalized_name, self.cfg.type_naming.indicator_prefixes):
            add_issue(
                "FLG_/IND_ column uses a type incompatible with indicator semantics",
                mismatch=not self._is_indicator_compatible_type(data_type, data_length, data_scale),
                limit=self._build_indicator_type_description(),
                length=f"{data_type}({'' if data_length is None else int(data_length)})",
            )

        if self._matches_any_prefix(normalized_name, self.cfg.type_naming.value_prefixes):
            add_issue(
                "VLR_ column should use a numeric type",
                mismatch=not self._is_numeric_type(data_type),
                limit="numeric",
                length=data_type,
            )

        if self._matches_any_prefix(normalized_name, self.cfg.type_naming.quantity_prefixes):
            quantity_scales = {float(value) for value in self.cfg.type_naming.quantity_numeric_scales}
            add_issue(
                "QTD_ column should use numeric type with integer scale",
                mismatch=(not self._is_numeric_type(data_type)) or (data_scale not in quantity_scales),
                limit=f"numeric scale in {sorted(int(value) for value in quantity_scales)}",
                length="" if data_scale is None else int(data_scale),
            )

        if self._matches_any_prefix(normalized_name, self.cfg.type_naming.code_prefixes):
            is_bad_text_type = data_type in self.TYPE_NAMING_LARGE_TEXT_TYPES
            is_very_long_text = (
                self._is_text_type(data_type)
                and data_length is not None
                and data_length > self.cfg.type_naming.code_max_text_length
            )
            add_issue(
                "COD_ column should not use very long text type",
                mismatch=is_bad_text_type or is_very_long_text,
                limit=f"<= {self.cfg.type_naming.code_max_text_length} chars",
                length="" if data_length is None else int(data_length),
            )

        for rule in self.cfg.type_naming.named_length_rules:
            if not re.search(rf"(^|_){re.escape(rule.token)}($|_)", normalized_name, flags=re.IGNORECASE):
                continue

            mismatch = not self._is_text_type(data_type)
            if not mismatch:
                mismatch = self._violates_named_length_rule(rule, data_length)
            add_issue(
                f"{rule.token} column type/size does not match expected convention",
                mismatch=mismatch,
                limit=rule.expected_description,
                length="" if data_length is None else int(data_length),
            )

        return issues

    def _matches_any_prefix(self, normalized_name: str, prefixes: List[str]) -> bool:
        upper_name = normalized_name.upper()
        return any(upper_name.startswith(prefix.upper()) for prefix in prefixes)

    def _build_indicator_type_description(self) -> str:
        text_lengths = "/".join(str(value) for value in self.cfg.type_naming.indicator_text_lengths)
        numeric_lengths = "/".join(str(value) for value in self.cfg.type_naming.indicator_numeric_lengths)
        numeric_scales = "/".join(str(value) for value in self.cfg.type_naming.indicator_numeric_scales)
        return f"CHAR/VARCHAR2({text_lengths}) or NUMBER({numeric_lengths},{numeric_scales})"

    def _violates_named_length_rule(self, rule: NamedLengthRule, data_length: float | None) -> bool:
        if data_length is None:
            return False
        if rule.allowed_lengths:
            return int(data_length) not in set(rule.allowed_lengths)
        if rule.max_length is not None:
            return int(data_length) > int(rule.max_length)
        return False
    # ------------------------- rule definitions ------------------------
    # Each rule is defined as a function that takes the full DataFrame and returns a boolean Series indicating which rows violate the rule. The rules are then applied in bulk using the apply_rules function, which adds columns for each rule indicating whether it was violated.
    # The individual issues are collected into lists during the validation steps, and then combined with the rule-based issues at the end to produce a comprehensive DataFrame of all metadata issues.
    # TO-DO
    def _build_rules(self) -> List[Rule]:
        #def length_required_missing(df: pd.DataFrame) -> pd.Series:
        #    mask = self._is_length_required()
        #    length = df.get("DATA_LENGTH")
        #    missing = length.isna() | (length <= 0)
        #    return mask & missing

        def data_scale_negative(df: pd.DataFrame) -> pd.Series:
            if "DATA_SCALE" not in df.columns:
                return pd.Series([False] * len(df), index=df.index)
            return df["DATA_SCALE"] < 0

        def num_distinct_negative(df: pd.DataFrame) -> pd.Series:
            if "NUM_DISTINCT" not in df.columns:
                return pd.Series([False] * len(df), index=df.index)
            return df["NUM_DISTINCT"] < 0

        return [
            Rule("MQME020", "Negative data scale", data_scale_negative),
            Rule("MQME021", "Negative num_distinct", num_distinct_negative),
        ]

    def _match_format_conformity_rule(self, column_name: str, data_type: str) -> SemanticFormatRuleSpec | None:
        normalized_type = str(data_type).strip().upper()
        if normalized_type not in (self.FORMAT_CONFORMITY_TEXT_TYPES | self.FORMAT_CONFORMITY_DATE_TYPES):
            return None

        normalized_name = self._normalize_semantic_name(column_name)
        for spec in SEMANTIC_FORMAT_RULES:
            for pattern in spec.name_patterns:
                if re.search(pattern, normalized_name, flags=re.IGNORECASE):
                    return spec
        return None

    def _normalize_semantic_name(self, column_name: str) -> str:
        normalized = re.sub(r"[^A-Z0-9]+", "_", str(column_name).upper())
        return normalized.strip("_")

    def _is_redundancy_candidate(self, row: pd.Series) -> bool:
        if bool(row.get("IS_PK", False)) or bool(row.get("IS_UNIQUE", False)):
            return False

        data_type = str(row.get("DATA_TYPE", "")).strip().upper()
        if data_type not in self.REDUNDANCY_TEXT_TYPES:
            return False

        normalized_name = self._normalize_semantic_name(str(row.get("COLUMN_NAME", "")))
        if not normalized_name:
            return False

        tokens = {token for token in normalized_name.split("_") if token}
        if tokens & self.REDUNDANCY_NEGATIVE_NAME_TOKENS:
            return False

        return bool(tokens & self.REDUNDANCY_POSITIVE_NAME_TOKENS)

    def _has_distinct_statistics(self, row: pd.Series) -> bool:
        num_rows = pd.to_numeric(pd.Series([row.get("NUM_ROWS", None)]), errors="coerce").iloc[0]
        num_nulls = pd.to_numeric(pd.Series([row.get("NUM_NULLS", None)]), errors="coerce").iloc[0]
        num_distinct = pd.to_numeric(pd.Series([row.get("NUM_DISTINCT", None)]), errors="coerce").iloc[0]
        return pd.notna(num_rows) and pd.notna(num_nulls) and pd.notna(num_distinct) and float(num_rows) > 0

    def _combine_issues(self, df_metadata: pd.DataFrame) -> pd.DataFrame:
        buckets = [
            self.list_tab_plural,
            self.list_tab_name_too_long,
            self.list_col_pref_no_standard,
            self.list_col_name_too_long,
            self.list_col_comment_missing,
            self.list_pk_bad_prefix,
            self.list_fk_bad_prefix,
            self.list_unique_bad_suffix,
            self.list_table_without_pk,
            self.list_table_without_integrity_constraint,
            self.list_identifier_not_protected,
            self.list_type_naming_mismatch,
        ]
        rows = []
        for b in buckets:
            rows.extend(b)
        df_model = (
            pd.DataFrame(rows, columns=df_metadata.columns).fillna("")
            if rows
            else pd.DataFrame(columns=df_metadata.columns)
        )
        return pd.concat([df_metadata, df_model], ignore_index=True)
