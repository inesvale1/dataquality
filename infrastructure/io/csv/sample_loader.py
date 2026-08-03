from __future__ import annotations

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
        for entry in self.base_folder.iterdir():
            if not entry.is_dir():
                continue
            schema_name = self._sanitize_name(entry.name)
            if not schema_name:
                continue
            inputs_dir = entry / "inputs"
            search_dir = inputs_dir if inputs_dir.is_dir() else entry
            self._load_dir(search_dir, schema_name, schemas)
        return schemas

    def _load_dir(self, directory: Path, schema_name: str, schemas: Dict[str, Dict[str, pd.DataFrame]]) -> None:
        for file_path in directory.glob("*.csv"):
            fname = file_path.name
            if fname.lower().startswith("metadata_"):
                continue
            table_name = self._table_name_from_file(fname)
            if not table_name:
                continue
            df = self._read_csv_with_fallback(file_path)
            df.columns = [str(c).strip().upper() for c in df.columns]
            schemas.setdefault(schema_name, {})[table_name] = df

    def _sanitize_name(self, name: str) -> str:
        return re.sub(r"[^0-9A-Z_]+", "_", str(name).upper()).strip("_")

    def _table_name_from_file(self, fname: str) -> str:
        stem = Path(fname).stem.upper()
        if stem.count(".") >= 2:
            return ""
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
