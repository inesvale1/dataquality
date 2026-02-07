from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import re

from dataquality.domain.config.validation_config import ValidationConfig
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

    def __init__(self, df: pd.DataFrame, table_plural_exceptions: List[str], config: ValidationConfig | None = None):
        self.df = df.copy()
        self.cfg = config or ValidationConfig()
        self.table_plural_exceptions = table_plural_exceptions

        required = {
            "OWNER",
            "TABLE_NAME",
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

    # ----------------------------- public API -----------------------------
    def run_all(self) -> pd.DataFrame:
        self._validate_tables()
        self._validate_columns()
        self._validate_constraints()
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

    def _validate_tables(self) -> None:
        plural_mask = self.df["TABLE_NAME"].str.upper().str.endswith("S")
        for _, row in self.df[plural_mask].drop_duplicates(subset=["OWNER", "TABLE_NAME"]).iterrows():
            table_name = row["TABLE_NAME"].upper()
            if table_name in [t.upper() for t in self.table_plural_exceptions]:
                continue
            self.list_tab_plural.append(
                {
                    "rule": "MQME20",
                    "desc": "Total number of tables with plural names",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                }
            )

        too_long_mask = self.df["TABLE_NAME"].str.len() > self.cfg.max_table_len
        for _, row in self.df[too_long_mask].drop_duplicates(subset=["OWNER", "TABLE_NAME"]).iterrows():
            self.list_tab_name_too_long.append(
                {
                    "rule": "MQME21",
                    "desc": "Total number of tables with names longer than recommended",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "length": int(len(row["TABLE_NAME"])),
                    "limit": self.cfg.max_table_len,
                }
            )

    def _validate_columns(self) -> None:
        allowed = tuple(self.cfg.prefix_names)
        bad_prefix = ~self.df["COLUMN_NAME"].str.upper().str.startswith(allowed)
        for _, row in self.df[bad_prefix].iterrows():
            self.list_col_pref_no_standard.append(
                {
                    "rule": "MQME22",
                    "desc": "Total number of tables with non-standard column prefixes",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "column": row["COLUMN_NAME"],
                }
            )

        too_long = self.df["COLUMN_NAME"].str.len() > self.cfg.max_column_len
        for _, row in self.df[too_long].iterrows():
            self.list_col_name_too_long.append(
                {
                    "rule": "MQME23",
                    "desc": "Total number of tables with column names longer than recommended",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "column": row["COLUMN_NAME"],
                    "length": int(len(row["COLUMN_NAME"])),
                    "limit": self.cfg.max_column_len,
                }
            )

        missing_comment = self._missing_text(self.df["COMMENTS"])
        for _, row in self.df[missing_comment].iterrows():
            self.list_col_comment_missing.append(
                {
                    "rule": "MQME10",
                    "desc": "Columns without comments",
                    "owner": row["OWNER"],
                    "table": row["TABLE_NAME"],
                    "column": row["COLUMN_NAME"],
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
                    "rule": "MQME24",
                            "desc": "Total number of tables with non-standard primary key prefixes",
                            "owner": owner,
                            "table": table,
                            "column": column,
                            "constraint_name": offender,
                        }
                    )

            if row["IS_FK"]:
                fk_names = [c["name"] for c in enabled if c.get("type") == "FOREIGN KEY"]
                names_to_check = fk_names or [c["name"] for c in enabled]
                if not any(n.upper().startswith(self.cfg.fk_prefix.upper()) for n in names_to_check):
                    offender = names_to_check[0] if names_to_check else ""
                    self.list_fk_bad_prefix.append(
                        {
                    "rule": "MQME25",
                            "desc": "Total number of tables with non-standard foreign key prefixes",
                            "owner": owner,
                            "table": table,
                            "column": column,
                            "constraint_name": offender,
                        }
                    )

            if row["IS_UNIQUE"]:
                uq_names = [c["name"] for c in enabled if c.get("type") == "UNIQUE"]
                names_to_check = uq_names or [c["name"] for c in enabled]
                if not any(unique_pat.match(n) for n in names_to_check):
                    offender = names_to_check[0] if names_to_check else ""
                    self.list_unique_bad_suffix.append(
                        {
                    "rule": "MQME26",
                            "desc": "Total number of tables with non-standard unique key prefixes",
                            "owner": owner,
                            "table": table,
                            "column": column,
                            "constraint_name": offender,
                        }
                    )

    def _build_rules(self) -> List[Rule]:
        def missing_data_type(df: pd.DataFrame) -> pd.Series:
            return self._missing_text(df["DATA_TYPE"])

        def length_required_missing(df: pd.DataFrame) -> pd.Series:
            mask = self._is_length_required()
            length = df.get("DATA_LENGTH")
            missing = length.isna() | (length <= 0)
            return mask & missing

        def data_scale_negative(df: pd.DataFrame) -> pd.Series:
            if "DATA_SCALE" not in df.columns:
                return pd.Series([False] * len(df), index=df.index)
            return df["DATA_SCALE"] < 0

        def num_distinct_negative(df: pd.DataFrame) -> pd.Series:
            if "NUM_DISTINCT" not in df.columns:
                return pd.Series([False] * len(df), index=df.index)
            return df["NUM_DISTINCT"] < 0

        def num_nulls_negative(df: pd.DataFrame) -> pd.Series:
            if "NUM_NULLS" not in df.columns:
                return pd.Series([False] * len(df), index=df.index)
            return df["NUM_NULLS"] < 0

        def density_outside(df: pd.DataFrame) -> pd.Series:
            if "DENSITY" not in df.columns:
                return pd.Series([False] * len(df), index=df.index)
            return (df["DENSITY"] < 0) | (df["DENSITY"] > 1)

        return [
            Rule("MQME11", "Columns without data type", missing_data_type),
            Rule("MQME12", "Length-required types without data length", length_required_missing),
            Rule("MQME13", "Negative data scale", data_scale_negative),
            Rule("MQME14", "Negative num_distinct", num_distinct_negative),
            Rule("MQME15", "Negative num_nulls", num_nulls_negative),
            Rule("MQME16", "Density outside [0, 1]", density_outside),
        ]

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
