from __future__ import annotations

import re
from pathlib import Path
from typing import Protocol

import pandas as pd

from dataquality.infrastructure.io.csv.sample_loader import SampleDataLoader
from dataquality.infrastructure.io.secure_credentials import DatabaseConnectionSettings, build_database_engine
from dataquality.shared.telemetry import get_current_telemetry


class SampleSource(Protocol):
    def get_samples_for_schema(self, schema_name: str, candidates_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        ...


class CsvSampleSource:
    def __init__(self, base_folder: Path):
        self.base_folder = Path(base_folder)
        self._cache: dict[str, dict[str, pd.DataFrame]] | None = None

    def get_samples_for_schema(self, schema_name: str, candidates_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        telemetry = get_current_telemetry()
        if self._cache is None:
            if telemetry is not None:
                with telemetry.stage("samples.csv_cache_load", schema=schema_name):
                    loader = SampleDataLoader(self.base_folder)
                    self._cache = loader.get_dictionary()
            else:
                loader = SampleDataLoader(self.base_folder)
                self._cache = loader.get_dictionary()
        if telemetry is not None:
            telemetry.increment("sample_source_csv_hits", schema=str(schema_name).upper())
        return self._cache.get(str(schema_name).upper(), {})


class DatabaseSampleSource:
    def __init__(
        self,
        connection_uri: str | None = None,
        connection_settings: DatabaseConnectionSettings | None = None,
        db_type: str = "Oracle",
        authentication_type: str = "username_password",
        driver_class_name: str | None = None,
        sample_limit: int = 1000,
        query_template: str | None = None,
    ):
        self._connection_settings = connection_settings
        self._raw_connection_uri = connection_uri
        self._driver_class_name = driver_class_name
        self.db_type = db_type
        self.authentication_type = authentication_type
        self.sample_limit = int(sample_limit)
        self.query_template = query_template or self._default_query_template(db_type)

    def get_samples_for_schema(self, schema_name: str, candidates_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        telemetry = get_current_telemetry()
        if candidates_df is None or candidates_df.empty:
            return {}

        try:
            from sqlalchemy import text
        except ImportError as exc:
            raise RuntimeError(
                "Database sample source requires SQLAlchemy. Install it before using --sample-source database."
            ) from exc

        if self._connection_settings is not None:
            engine = build_database_engine(self._connection_settings)
        elif self._raw_connection_uri:
            try:
                from sqlalchemy import create_engine
            except ImportError as exc:
                raise RuntimeError("Database sample source requires SQLAlchemy.") from exc
            engine = create_engine(str(self._raw_connection_uri).strip())
        else:
            raise ValueError("connection_uri or connection_settings is required for database sample source.")
        samples_by_table: dict[str, pd.DataFrame] = {}
        unique_tables = (
            candidates_df[["OWNER", "TABLE_NAME"]]
            .drop_duplicates()
            .fillna("")
            .itertuples(index=False, name=None)
        )

        if telemetry is not None:
            with telemetry.stage("samples.oracle_load", schema=schema_name):
                with engine.connect() as connection:
                    for owner, table_name in unique_tables:
                        owner_name = str(owner or schema_name).upper()
                        normalized_table = str(table_name).upper()
                        query = self.query_template.format(
                            owner=owner_name,
                            table=normalized_table,
                            limit=self.sample_limit,
                        )
                        with telemetry.stage(
                            "oracle.sample_query",
                            schema=owner_name,
                            table=normalized_table,
                            extra={"sample_limit": self.sample_limit},
                        ):
                            telemetry.increment("oracle_queries_executed", schema=owner_name)
                            df = pd.read_sql(text(query), connection)
                        df.columns = [str(c).strip().upper() for c in df.columns]
                        samples_by_table[normalized_table] = df
                        telemetry.increment("sample_tables_loaded", schema=owner_name)
                        telemetry.increment("oracle_rows_returned", int(df.shape[0]), schema=owner_name)
                        telemetry.set_gauge("last_query_rows_returned", int(df.shape[0]), schema=owner_name)
        else:
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

    def _build_connection_uri(
        self,
        connection_uri: str | None,
        connection_settings: DatabaseConnectionSettings | None,
        driver_class_name: str | None,
    ) -> str:
        if connection_settings is not None:
            return build_database_connection_uri(connection_settings)
        if not connection_uri:
            raise ValueError("connection_uri or connection_settings is required for database sample source.")
        return self._normalize_connection_uri(connection_uri, driver_class_name)



class S3SampleSource:
    SAMPLE_PREFIXES = ("sample_", "samples_", "amostra_", "dados_", "data_")

    def __init__(self, uri: str, storage_options: dict[str, object] | None = None):
        self.uri = str(uri).strip().rstrip("/")
        self.storage_options = storage_options or {}
        self._cache: dict[str, dict[str, pd.DataFrame]] | None = None

    def get_samples_for_schema(self, schema_name: str, candidates_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        if not self.uri:
            raise ValueError("sample_s3_uri is required when sample_source='s3'.")
        if self._cache is None:
            self._cache = self._load_cache()
        return self._cache.get(str(schema_name).upper(), {})

    def _load_cache(self) -> dict[str, dict[str, pd.DataFrame]]:
        try:
            import fsspec
        except ImportError as exc:
            raise RuntimeError("S3 sample source requires fsspec/s3fs. Install it with: pip install s3fs") from exc

        fs, _, paths = fsspec.get_fs_token_paths(self.uri, storage_options=self.storage_options)
        base_path = paths[0].rstrip("/")
        matches = fs.glob(f"{base_path}/**/*.csv")
        result: dict[str, dict[str, pd.DataFrame]] = {}
        for path in matches:
            uri = f"s3://{path}" if not str(path).startswith("s3://") else str(path)
            parsed = self._parse_sample_filename(uri)
            if parsed is None:
                continue
            owner, table = parsed
            df = pd.read_csv(uri, storage_options=self.storage_options)
            df.columns = [str(column).strip().upper() for column in df.columns]
            result.setdefault(owner, {})[table] = df
        return result

    def _parse_sample_filename(self, uri: str) -> tuple[str, str] | None:
        path_text = str(uri).replace("\\", "/")
        file_name = path_text.rstrip("/").split("/")[-1]
        parent_name = path_text.rstrip("/").split("/")[-2] if "/" in path_text.rstrip("/") else ""
        stem = Path(file_name).stem.upper()
        if stem.startswith("METADADOS_"):
            return None

        dot_parts = [part.strip() for part in stem.split(".") if part.strip()]
        if len(dot_parts) >= 3:
            return self._sanitize_name(dot_parts[-3]), self._sanitize_name(dot_parts[-2])

        table_name = stem
        for prefix in self.SAMPLE_PREFIXES:
            prefix_upper = prefix.upper()
            if table_name.startswith(prefix_upper):
                table_name = table_name[len(prefix_upper):]
                break

        owner = self._sanitize_name(parent_name)
        table = self._sanitize_name(table_name)
        if not owner or not table:
            return None
        return owner, table

    def _sanitize_name(self, name: str) -> str:
        return re.sub(r"[^0-9A-Z_]+", "_", str(name).upper()).strip("_")
