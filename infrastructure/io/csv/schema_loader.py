from __future__ import annotations

import os
import re
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


class schemaLoader:
    """Load metadata CSV files (metadados_*.csv) from a base folder.

    The loader walks all subfolders under `base_folder`, finds CSV files matching
    `metadados_<schema>.csv`, loads each file into a typed DataFrame, and stores
    the result in a dictionary keyed by `<schema>`.

    This implementation is intentionally CSV-based, so you can later replace it
    with an Oracle-backed loader while keeping the same interface.
    """

    REQUIRED = [
        "OWNER",
        "TABLE_NAME",
        "NUM_ROWS",
        "COLUMN_NAME",
        "DATA_TYPE",
        "NULLABLE",
        "DEFAULT_ON_NULL",
        "CONSTRAINTS",
        "IS_PK",
        "IS_FK",
        "IS_UNIQUE",
    ]

    STRING_COLS = ["OWNER", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COMMENTS", "CONSTRAINTS", "DUPLICATED"]
    INT_COLS = [
        "NUM_ROWS",
        "COLUMN_ID",
        "DATA_LENGTH",
        "DATA_SCALE",
        "AVG_COL_LEN",
        "NUM_DISTINCT",
        "NUM_NULLS",
        "NUM_BUCKETS",
        "TABLE_ROWS",
        "ROW_COUNT",
    ]
    DOUBLE_COLS = []
    BOOL_COLS = ["NULLABLE", "DEFAULT_ON_NULL", "IS_PK", "IS_FK", "IS_UNIQUE"]

    _TRUE_TOKENS = {"Y", "YES", "SIM", "S", "1", "TRUE", "VERDADE", "VERDADEIRO", "T", "ON"}
    _FALSE_TOKENS = {"N", "NO", "NÃO", "NAO", "0", "FALSE", "FALSO", "F", "OFF"}

    def __init__(self, base_folder: Path, columns_to_delete: Optional[List[str]] = None):
        self.base_folder: Path = Path(base_folder)
        self.columns_to_delete = columns_to_delete or []
        self.dictionary: Dict[str, pd.DataFrame] = self._read_csv_tree()

    def get_dictionary(self) -> Dict[str, pd.DataFrame]:
        return self.dictionary

    # ---------------- internal helpers ----------------

    def _read_csv_tree(self) -> Dict[str, pd.DataFrame]:
        if not self.base_folder.exists():
            raise FileNotFoundError(f"Base folder not found: {self.base_folder}")

        dfs: Dict[str, pd.DataFrame] = {}

        # Agora o padrão aceita apenas CSV
        pattern = re.compile(r"^metadados_(.+)\.csv$", flags=re.IGNORECASE)

        for root, _, files in os.walk(self.base_folder):
            for fname in files:
                m = pattern.match(fname)
                if not m:
                    continue

                suffix_raw = m.group(1)
                suffix = self._sanitize_suffix(suffix_raw)

                csv_path = Path(root) / fname

                # segurança extra: garante que é CSV
                if csv_path.suffix.lower() != ".csv":
                    continue

                if fname and "sinfa2" in fname.lower():
                    print(f"Skipping file: {csv_path}")
                    continue

                df = self._load_and_typed_file(csv_path)

                if self.columns_to_delete:
                    df = df.drop(
                        columns=[c for c in self.columns_to_delete if c in df.columns],
                        errors="ignore"
                    )

                dfs[suffix] = df

        return dfs

    def _sanitize_suffix(self, suffix: str) -> str:
        return re.sub(r"[^0-9a-zA-Z_]+", "_", suffix).strip("_").lower()

    def _to_bool(self, v: Any) -> bool:
        if pd.isna(v):
            return False
        s = str(v).strip().upper()
        if s in self._TRUE_TOKENS:
            return True
        if s in self._FALSE_TOKENS:
            return False
        # fallback: numeric truthiness
        try:
            return bool(int(float(s)))
        except Exception:
            return False

    def _read_csv_with_fallback(self,path):
        encodings = ["utf-8-sig", "cp1252", "latin1"]
        seps = [",", ";", "\t", "|"]

        last_err: Exception | None = None
        for enc in encodings:
            for sep in seps:
                try:
                    df = pd.read_csv(path, encoding=enc, sep=sep, quotechar='"')
                    # Heurística: se leu apenas 1 coluna e o header contém ;, provavelmente separador errado
                    if df.shape[1] == 1 and ";" in df.columns[0]:
                        continue
                    return df
                except (UnicodeDecodeError, pd.errors.ParserError) as e:
                    last_err = e

        raise last_err if last_err else RuntimeError(f"Could not read CSV: {path}")
    
    def _load_and_typed_file(self, path: Path) -> pd.DataFrame:
        #df = pd.read_csv(path) if path.suffix.lower()=='.csv' else pd.read_excel(path)
        df = self._read_csv_with_fallback(path) if path.suffix.lower() == ".csv" else pd.read_excel(path)
        print(path)
        # Normalize headers to UPPER + strip
        df.columns = [c.strip().upper() for c in df.columns]

        # Normalizar: converter para string e maiusculas
        df['CONSTRAINTS_NORM'] = df['CONSTRAINTS'].astype(str).str.upper().fillna("")

        #print(f"Loaded {path} with columns: {df.columns.tolist()}")

        # Criar as flags
        df['IS_PK']     = df['CONSTRAINTS_NORM'].str.contains('PRIMARY KEY', na=False)
        df['IS_FK']     = df['CONSTRAINTS_NORM'].str.contains('FOREIGN KEY', na=False)
        df['IS_UNIQUE'] = df['CONSTRAINTS_NORM'].str.contains('UNIQUE', na=False)

        missing = [c for c in self.REQUIRED if c not in df.columns]
        if missing:
            raise ValueError(f"File {path.name} is missing required columns: {missing}")

        # (Optional) delete auxiliary column
        df.drop(columns=['CONSTRAINTS_NORM'], inplace=True)
        
        # Ensure optional cols exist
        for c in (set(self.STRING_COLS + self.INT_COLS + self.DOUBLE_COLS + self.BOOL_COLS) - set(df.columns)):
            df[c] = pd.NA

        # Coerce types
        for c in self.STRING_COLS:
            if c in df.columns:
                df[c] = df[c].astype(str)

        for c in self.INT_COLS:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        for c in self.DOUBLE_COLS:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype(float)

        for c in self.BOOL_COLS:
            if c in df.columns:
                df[c] = df[c].map(self._to_bool).astype(bool)

        desired = self.STRING_COLS + self.INT_COLS + self.DOUBLE_COLS + self.BOOL_COLS
        ordered = [c for c in desired if c in df.columns] + [c for c in df.columns if c not in desired]
        return df[ordered]
