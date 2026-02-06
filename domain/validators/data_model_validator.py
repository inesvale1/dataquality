from __future__ import annotations
import pandas as pd

import re
from typing import Any, Dict, List

from dataquality.domain.config.validation_config import ValidationConfig

class DataModelValidator:
    """
    Validates table/column/constraint naming and documentation rules over a typed DataFrame.
    """

    # --- public “reports” you can inspect after run_all() ---
    def __init__(self, df: pd.DataFrame, table_plural_exceptions: List[str], config: ValidationConfig | None = None):
        self.df             = df.copy()
        self.cfg            = config or ValidationConfig()
        self.number_tables  = df['TABLE_NAME'].nunique()
        self.number_columns = self.df.shape[0]
        self.number_pks     = df['IS_PK'].sum()
        self.number_fks     = df['IS_FK'].sum()
        self.number_uks     = df['IS_UNIQUE'].sum()
        self.number_tables_plural         = 0
        self.number_tables_longer_names   = 0
        self.number_columns_prefixes      = 0
        self.number_columns_longer_names  = 0
        self.number_columns_comments      = 0
        self.number_PK_prefixes           = 0
        self.number_FK_prefixes           = 0
        self.number_UK_prefixes           = 0

        # list of exception tables for the plural
        self.table_plural_exceptions = table_plural_exceptions

        # Lists of rule violations
        self.list_tab_plural: List[Dict[str, Any]] = []
        self.list_tab_name_too_long: List[Dict[str, Any]] = []

        self.list_col_pref_no_standard: List[Dict[str, Any]] = []
        self.list_col_name_too_long: List[Dict[str, Any]] = []
        self.list_col_comment_missing: List[Dict[str, Any]] = []

        self.list_pk_bad_prefix: List[Dict[str, Any]] = []
        self.list_fk_bad_prefix: List[Dict[str, Any]] = []
        self.list_unique_bad_suffix: List[str] = []

        # Consolidated issues table
        self.issues_df: pd.DataFrame | None = None

        # Basic sanity: required columns present?
        required = {
            "OWNER","TABLE_NAME","COLUMN_NAME","DATA_TYPE","NULLABLE",
            "DEFAULT_ON_NULL","IS_PK","IS_FK","IS_UNIQUE","CONSTRAINTS"
        }
        missing = [c for c in required if c not in self.df.columns]
        if missing:
            raise ValueError(f"Input DataFrame is missing required columns: {missing}")

        # Normalize some strings for safer checks
        for col in ("OWNER","TABLE_NAME","COLUMN_NAME","DATA_TYPE"):
            self.df[col] = self.df[col].astype(str).str.strip()
        if "COMMENTS" in self.df.columns:
            self.df["COMMENTS"] = self.df["COMMENTS"].astype(str)

    # ---------------- accessory methods (primary getters) ----------------
    def get_number_tables(self):
        return self.number_tables
    def get_number_columns(self):
        return self.number_columns
    def get_number_primary_keys(self):
        return self.number_pks
    def get_number_foreign_keys(self):
        return self.number_fks
    def get_number_unique_keys(self):
        return self.number_uks

    # ---------------- accessory methods (secundary getters) ---------------
    def get_number_tables_plural(self):
        return self.number_tables_plural
    def get_number_tables_longer_names(self):
        return self.number_tables_longer_names
    def get_number_columns_prefixes(self):
        return self.number_columns_prefixes
    def get_number_columns_longer_names(self):
        return self.number_columns_longer_names
    def get_number_columns_comments(self):
        return self.number_columns_comments
    def get_number_PK_prefixes(self):
        return self.number_PK_prefixes
    def get_number_FK_prefixes(self):
        return self.number_FK_prefixes
    def get_number_UK_prefixes(self):
        return self.number_UK_prefixes

    # ----------------------------- public API -----------------------------

    def run_all(self) -> pd.DataFrame:
        """Run all validations and return a consolidated issues DataFrame."""
        self._validate_tables()
        self._validate_columns()
        self._validate_constraints()
        self._set_attributes()

        self.issues_df = self._combine_issues()

        return self.issues_df

    # ------------------------- validation helpers ------------------------

    def _validate_tables(self) -> None:
        # 1.1 Name must not end with 'S' (plural)  [case-insensitive]
        plural_mask = self.df["TABLE_NAME"].str.upper().str.endswith("S")
        for _, row in self.df[plural_mask].drop_duplicates(subset=["OWNER","TABLE_NAME"]).iterrows():
            table_name = row["TABLE_NAME"].upper()
            if table_name in [t.upper() for t in self.table_plural_exceptions]:
              continue
            self.list_tab_plural.append({
              "rule": "MQMD06",
              "desc": "Total number of tables with plural names", # 1.1_table_plural
              "owner": row["OWNER"],
              "table": row["TABLE_NAME"]
            })

        # 1.2 Name length must be <= max_table_len
        too_long_mask = self.df["TABLE_NAME"].str.len() > self.cfg.max_table_len
        for _, row in self.df[too_long_mask].drop_duplicates(subset=["OWNER","TABLE_NAME"]).iterrows():
            self.list_tab_name_too_long.append({
              "rule": "MQMD07",
              "desc": "Total number of tables with names longer than recommended", # 1.2_table_name_too_long
              "owner": row["OWNER"],
              "table": row["TABLE_NAME"],
              "length": int(len(row["TABLE_NAME"])),
              "limit": self.cfg.max_table_len
            })

    # 2.3 Non-null comment per column (must not be empty/whitespace/null-tokens)
    def _missing_text(self, s: pd.Series) -> pd.Series:
        # mantém NaN como NaN; só depois normaliza texto
        s_str = (
            s.astype("string")                           # preserva <NA>
            .str.replace("\u00A0", " ", regex=False)    # NBSP -> espaço
            .str.strip()
        )
        null_tokens = {"", "nan", "<na>", "none", "null", "n/a", "na"}
        return s_str.isna() | s_str.str.lower().isin(null_tokens)

    def _validate_columns(self) -> None:
        # 2.1 Column prefix must be one of allowed
        allowed = tuple(self.cfg.prefix_names)
        bad_prefix = ~self.df["COLUMN_NAME"].str.upper().str.startswith(allowed)
        for _, row in self.df[bad_prefix].iterrows():
            self.list_col_pref_no_standard.append({
              "rule": "MQMD08",
              "desc": "Total number of tables with non-standard column prefixes", # 2.1_column_prefix
              "owner": row["OWNER"],
              "table": row["TABLE_NAME"],
              "column": row["COLUMN_NAME"]
            })

        # 2.2 Column name length <= max_column_len
        too_long = self.df["COLUMN_NAME"].str.len() > self.cfg.max_column_len
        for _, row in self.df[too_long].iterrows():
            self.list_col_name_too_long.append({
              "rule": "MQMD09",
              "desc": "Total number of tables with column names longer than recommended", # 2.2_column_name_too_long
              "owner": row["OWNER"],
              "table": row["TABLE_NAME"],
              "column": row["COLUMN_NAME"],
              "length": int(len(row["COLUMN_NAME"])),
              "limit": self.cfg.max_column_len
            })

        # 2.3 Non-null comment per column (not empty/whitespace)
        missing_comment = self._missing_text(self.df["COMMENTS"])

        for _, row in self.df[missing_comment].iterrows():
            self.list_col_comment_missing.append({
              "rule": "MQMD10",
              "desc": "Total number of tables with column names without comments", # 2.3_column_comment_missing
              "owner": row["OWNER"],
              "table": row["TABLE_NAME"],
              "column": row["COLUMN_NAME"]
            })

    def _parse_constraints(self, constraints_cell: Any) -> List[Dict[str, Any]]:
      """
      Parseia o conteúdo da coluna CONSTRAINTS (string por linha) no formato:

          "PK_ARQUIVO_PAGAMENTO (PRIMARY KEY, ENABLED);
          SYS_C001528284 (CHECK, ENABLED)"

      Retorna uma lista de dicts:
          [{"name": str, "type": str, "enabled": bool}, ...]

      - Ignora partes vazias ou do tipo "(, )".
      - Reconhece tipos: PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK.
      - Assume enabled=True se encontrar 'ENABLED' e não encontrar 'DISABLED'.
      """
      result: List[Dict[str, Any]] = []

      # Se vier None, NaN ou não-for-string, não há o que parsear
      if not isinstance(constraints_cell, str):
          return result

      text = constraints_cell.strip()
      if not text:
          return result

      # Divide por ';' cada definição de constraint
      parts = [p.strip() for p in text.split(';') if p.strip()]

      # Regex para capturar: NOME (detalhes)
      # Ex.: "PK_ARQUIVO_PAGAMENTO (PRIMARY KEY, ENABLED)"
      pattern = re.compile(r'^\s*([^\s(]+)\s*\(([^)]*)\)\s*$', re.IGNORECASE)

      for part in parts:
          # Exemplo de lixo: "(, )" -> não tem nome antes do parêntese
          m = pattern.match(part)
          if not m:
              continue

          name = m.group(1).strip()
          details = m.group(2).strip()
          if not name:
              continue

          details_upper = details.upper()

          # Descobrir o tipo
          ctype = None
          for candidate in ("PRIMARY KEY", "FOREIGN KEY", "UNIQUE"):
              if candidate in details_upper:
                  ctype = candidate
                  break

          # Enabled/Disabled
          # (Oracle costuma listar ", ENABLED" / ", DISABLED")
          enabled = ("ENABLED" in details_upper) and ("DISABLED" not in details_upper)

          result.append(
              {
                  "name": name,
                  "type": ctype,     # pode ser None se não encontrar nenhum dos tipos conhecidos
                  "enabled": enabled
              }
          )

      return result

    def _validate_constraints(self) -> None:
      """
      3.1 PK name must start with 'PK_'
      3.2 FK name must start with 'FK_'
      3.3 UNIQUE name must end with _UK or _UK<digits>
      Usa as colunas IS_PK, IS_FK, IS_UNIQUE e o campo CONSTRAINTS.
      """

      unique_pat = re.compile(self.cfg.unique_suffix_regex, flags=re.IGNORECASE)

      # Garante bool nas flags, evita NaN/None atrapalhando
      df = self.df.copy()
      for flag in ["IS_PK", "IS_FK", "IS_UNIQUE"]:
          if flag in df.columns:
              df[flag] = df[flag].fillna(False).astype(bool)
          else:
              df[flag] = False

      # Só analisa linhas que têm pelo menos uma dessas flags = True
      mask_has_constraints = df[["IS_PK", "IS_FK", "IS_UNIQUE"]].any(axis=1)
      df_to_check = df[mask_has_constraints]

      for _, row in df_to_check.iterrows():
          owner  = row["OWNER"]
          table  = row["TABLE_NAME"]
          column = row["COLUMN_NAME"]

          # Parse do texto de CONSTRAINTS dessa linha
          raw_constraints = row.get("CONSTRAINTS", "") or ""
          items = self._parse_constraints(raw_constraints)  # list[{"name": ..., "type": ..., "enabled": ...}]
          enabled = [c for c in items if c.get("enabled") is True]

          # Se não tem nada habilitado, não há o que validar
          if not enabled:
              continue

          # ---------------- PK ----------------
          if row["IS_PK"]:
              pk_names = [c["name"] for c in enabled if c.get("type") == "PRIMARY KEY"]
              # Se não achar nada tipado como PRIMARY KEY, tenta qualquer constraint habilitada
              names_to_check = pk_names or [c["name"] for c in enabled]

              if not any(n.upper().startswith(self.cfg.pk_prefix.upper()) for n in names_to_check):
                  offender = names_to_check[0] if names_to_check else ""
                  self.list_pk_bad_prefix.append({
                    "rule": "MQMD11",
                    "desc": "Total number of tables with non-standard primary key prefixes",
                    "owner": owner,
                    "table": table,
                    "column": column,
                    "constraint_name": offender
                  })

          # ---------------- FK ----------------
          if row["IS_FK"]:
              fk_names = [c["name"] for c in enabled if c.get("type") == "FOREIGN KEY"]
              names_to_check = fk_names or [c["name"] for c in enabled]

              if not any(n.upper().startswith(self.cfg.fk_prefix.upper()) for n in names_to_check):
                  offender = names_to_check[0] if names_to_check else ""
                  self.list_fk_bad_prefix.append({
                    "rule": "MQMD12",
                    "desc": "Total number of tables with non-standard foreign key prefixes",
                    "owner": owner,
                    "table": table,
                    "column": column,
                    "constraint_name": offender
                  })

          # ---------------- UNIQUE ----------------
          if row["IS_UNIQUE"]:
              uq_names = [c["name"] for c in enabled if c.get("type") == "UNIQUE"]
              names_to_check = uq_names or [c["name"] for c in enabled]

              if not any(unique_pat.match(n) for n in names_to_check):
                  offender = names_to_check[0] if names_to_check else ""
                  self.list_unique_bad_suffix.append({
                    "rule": "MQMD13",
                    "desc": "Total number of tables with non-standard unique key prefixes",
                    "owner": owner,
                    "table": table,
                    "column": column,
                    "constraint_name": offender
                  })

    def _set_attributes(self) -> None:
      pairs = [
          ('number_tables_plural', self.list_tab_plural),
          ('number_tables_longer_names', self.list_tab_name_too_long),
          ('number_columns_prefixes', self.list_col_pref_no_standard),
          ('number_columns_longer_names', self.list_col_name_too_long),
          ('number_columns_comments', self.list_col_comment_missing),
          ('number_PK_prefixes', self.list_pk_bad_prefix),
          ('number_FK_prefixes', self.list_fk_bad_prefix),
          ('number_UK_prefixes', self.list_unique_bad_suffix)
      ]

      for attr_name, value_list in pairs:
          setattr(self, attr_name, len(value_list) if value_list else 0)

    def _combine_issues(self) -> pd.DataFrame:
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
      cols = ["rule","desc","owner","table","column","constraint_name","length","limit"]
      return pd.DataFrame(rows, columns=cols).fillna("") if rows else pd.DataFrame(columns=cols)
