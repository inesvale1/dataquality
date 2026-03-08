from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict

import pandas as pd


class SampleDataLoader:
    SAMPLE_PREFIXES = ("sample_", "samples_", "amostra_", "dados_", "data_")

    def __init__(self, base_folder: Path):
        self.base_folder = Path(base_folder)

    def get_dictionary(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        if not self.base_folder.exists():
            raise FileNotFoundError(f"Base folder not found: {self.base_folder}")

        schemas: Dict[str, Dict[str, pd.DataFrame]] = {}
        for root, _, files in os.walk(self.base_folder):
            root_path = Path(root)
            schema_name = self._sanitize_name(root_path.name)
            if not schema_name:
                continue

            for fname in files:
                if not fname.lower().endswith(".csv"):
                    continue
                if fname.lower().startswith("metadados_"):
                    continue

                file_path = root_path / fname
                table_name = self._table_name_from_file(fname)
                if not table_name:
                    continue

                df = self._read_csv_with_fallback(file_path)
                df.columns = [str(c).strip().upper() for c in df.columns]
                schemas.setdefault(schema_name, {})[table_name] = df

        return schemas

    def _sanitize_name(self, name: str) -> str:
        return re.sub(r"[^0-9A-Z_]+", "_", str(name).upper()).strip("_")

    def _table_name_from_file(self, fname: str) -> str:
        stem = Path(fname).stem.upper()
        for prefix in self.SAMPLE_PREFIXES:
            prefix_upper = prefix.upper()
            if stem.startswith(prefix_upper):
                stem = stem[len(prefix_upper):]
                break
        return self._sanitize_name(stem)

    def _read_csv_with_fallback(self, path: Path) -> pd.DataFrame:
        encodings = ["utf-8-sig", "cp1252", "latin1"]
        separators = [",", ";", "\t", "|"]
        last_err: Exception | None = None

        for enc in encodings:
            for sep in separators:
                try:
                    df = pd.read_csv(path, encoding=enc, sep=sep, quotechar='"')
                    if df.shape[1] == 1 and ";" in str(df.columns[0]):
                        continue
                    return df
                except (UnicodeDecodeError, pd.errors.ParserError) as exc:
                    last_err = exc

        raise last_err if last_err else RuntimeError(f"Could not read CSV: {path}")
