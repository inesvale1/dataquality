from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
import re


@dataclass(frozen=True)
class DocumentCodeInputFile:
    owner: str
    table: str
    column: str
    path: Path


class DocumentCodeLoader:
    def __init__(self, base_folder: Path):
        self.base_folder = Path(base_folder)

    def list_schema_files(self, schema_name: str) -> list[DocumentCodeInputFile]:
        schema_key = str(schema_name or "").strip().upper()
        if not self.base_folder.exists():
            return []

        result: list[DocumentCodeInputFile] = []
        for path in self.base_folder.rglob("*.csv"):
            parsed = self._parse_input_filename(path.name)
            if parsed is None:
                continue
            owner, table, column = parsed
            if owner != schema_key:
                continue
            result.append(
                DocumentCodeInputFile(
                    owner=owner,
                    table=table,
                    column=column,
                    path=path,
                )
            )
        return sorted(result, key=lambda item: (item.owner, item.table, item.column, str(item.path)))

    def load_values(self, input_file: DocumentCodeInputFile) -> tuple[str, list[str]]:
        header, values = self._read_single_column_csv_with_fallback(input_file.path)
        column_name = header or input_file.column
        return column_name, values

    def _parse_input_filename(self, filename: str) -> tuple[str, str, str] | None:
        lowered = filename.lower()
        if lowered == "metadados.csv" or lowered.startswith("metadata_"):
            return None

        stem = Path(filename).stem
        parts = [part.strip() for part in stem.split(".") if part.strip()]
        if len(parts) < 3:
            return None

        owner, table, column = parts[-3:]
        if not owner or not table or not column:
            return None
        return owner.upper(), table.upper(), column.upper()

    def _read_single_column_csv_with_fallback(self, path: Path) -> tuple[str, list[str]]:
        encodings = ["utf-8-sig", "cp1252", "latin1"]
        separators = [",", ";", "\t", "|"]

        last_error: Exception | None = None
        for encoding in encodings:
            for separator in separators:
                try:
                    with path.open("r", encoding=encoding, newline="") as handle:
                        reader = csv.reader(handle, delimiter=separator, quotechar='"')
                        rows = list(reader)
                    if not rows:
                        return "", []
                    header_row = [re.sub(r"^\ufeff", "", str(col)).strip().upper() for col in rows[0]]
                    if len(header_row) == 1 and any(token in header_row[0] for token in [";", "\t", "|"]):
                        continue
                    values: list[str] = []
                    for row in rows[1:]:
                        if not row:
                            continue
                        values.append("" if row[0] is None else str(row[0]).strip())
                    return (header_row[0] if header_row else "", values)
                except (UnicodeDecodeError, csv.Error) as exc:
                    last_error = exc

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Could not read CSV: {path}")
