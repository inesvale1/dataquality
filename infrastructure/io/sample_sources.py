from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pandas as pd

from dataquality.infrastructure.io.csv.sample_loader import SampleDataLoader


class SampleSource(Protocol):
    def get_samples_for_schema(self, schema_name: str, candidates_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        ...


class CsvSampleSource:
    def __init__(self, base_folder: Path):
        self.base_folder = Path(base_folder)
        self._cache: dict[str, dict[str, pd.DataFrame]] | None = None

    def get_samples_for_schema(self, schema_name: str, candidates_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        if self._cache is None:
            loader = SampleDataLoader(self.base_folder)
            self._cache = loader.get_dictionary()
        return self._cache.get(str(schema_name).upper(), {})


class DatabaseSampleSource:
    def __init__(
        self,
        connection_uri: str,
        db_type: str = "Oracle",
        authentication_type: str = "username_password",
        driver_class_name: str | None = None,
        sample_limit: int = 1000,
        query_template: str | None = None,
    ):
        self.connection_uri = self._normalize_connection_uri(connection_uri, driver_class_name)
        self.db_type = db_type
        self.authentication_type = authentication_type
        self.driver_class_name = driver_class_name
        self.sample_limit = int(sample_limit)
        self.query_template = query_template or self._default_query_template(db_type)

    def get_samples_for_schema(self, schema_name: str, candidates_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        if candidates_df is None or candidates_df.empty:
            return {}

        try:
            from sqlalchemy import create_engine, text
        except ImportError as exc:
            raise RuntimeError(
                "Database sample source requires SQLAlchemy. Install it before using --sample-source database."
            ) from exc

        engine = create_engine(self.connection_uri, connect_args=self._build_connect_args())
        samples_by_table: dict[str, pd.DataFrame] = {}
        unique_tables = (
            candidates_df[["OWNER", "TABLE_NAME"]]
            .drop_duplicates()
            .fillna("")
            .itertuples(index=False, name=None)
        )

        with engine.connect() as connection:
            for owner, table_name in unique_tables:
                owner_name = str(owner or schema_name).upper()
                normalized_table = str(table_name).upper()
                query = self.query_template.format(
                    owner=owner_name,
                    table=normalized_table,
                    limit=self.sample_limit,
                )
                df = pd.read_sql(text(query), connection)
                df.columns = [str(c).strip().upper() for c in df.columns]
                samples_by_table[normalized_table] = df

        return samples_by_table

    def _default_query_template(self, db_type: str) -> str:
        db_type_normalized = str(db_type).strip().lower()
        if db_type_normalized == "oracle":
            return "SELECT * FROM {owner}.{table} FETCH FIRST {limit} ROWS ONLY"
        return "SELECT * FROM {owner}.{table} LIMIT {limit}"

    def _normalize_connection_uri(self, connection_uri: str, driver_class_name: str | None) -> str:
        uri = str(connection_uri).strip()
        if "://" in uri or not driver_class_name:
            return uri
        return f"{driver_class_name}://{uri}"

    def _build_connect_args(self) -> dict[str, object]:
        auth_type = str(self.authentication_type).strip().lower()
        if auth_type in {"username_password", "password", "basic", ""}:
            return {}
        if auth_type in {"external", "kerberos", "iam"}:
            return {}
        raise ValueError(f"Unsupported authentication_type: {self.authentication_type}")
