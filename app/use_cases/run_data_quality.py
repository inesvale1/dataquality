from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dataquality.adapters.outbound.exporters.excel_report import save_excel_report
from dataquality.domain.config.validation_config import ValidationConfig
from dataquality.domain.validators.data_quality_validator import DataQualityValidator
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.infrastructure.io.csv.schema_loader import schemaLoader
from dataquality.infrastructure.io.sample_sources import CsvSampleSource, DatabaseSampleSource, SampleSource
from dataquality.shared.telemetry import get_current_telemetry

import pandas as pd


@dataclass
class RunDataQualityOptions:
    metadata_base_folder: Path
    columns_to_delete: List[str]
    plural_table_exceptions: List[str]
    sample_base_folder: Path | None = None
    validation_config: Optional[ValidationConfig] = None
    db_type: str = "Oracle"
    exclude_tables: List[str] | None = None
    sample_source_type: str = "csv"
    db_connection_uri: str | None = None
    db_authentication_type: str = "username_password"
    db_driver_class_name: str | None = None
    sample_query_template: str | None = None
    sample_limit: int = 1000


def run_data_quality(options: RunDataQualityOptions) -> None:
    print("\nSummary:")
    telemetry = get_current_telemetry()

    with (telemetry.stage("metadata.load") if telemetry is not None else nullcontext()):
        metadata_loader = schemaLoader(Path(options.metadata_base_folder), options.columns_to_delete)
        metadata_by_schema = metadata_loader.get_dictionary()
    sample_source = _build_sample_source(options)

    exclude_set = _parse_exclude_tables(options.exclude_tables or [])

    print(f"Total metadata schemas loaded: {len(metadata_by_schema)}")
    print(f"Sample source type: {options.sample_source_type}")
    if telemetry is not None:
        telemetry.set_metadata(
            use_case="run_data_quality",
            sample_source_type=options.sample_source_type,
            sample_limit=options.sample_limit,
        )
        telemetry.set_gauge("schemas_loaded", len(metadata_by_schema))

    dq_validator = DataQualityValidator()

    for schema_name, df_metadata in metadata_by_schema.items():
        with (telemetry.stage("schema.process", schema=schema_name) if telemetry is not None else nullcontext()):
            if exclude_set:
                df_metadata = _filter_excluded_tables(df_metadata, exclude_set)

            if telemetry is not None:
                telemetry.set_gauge("input_columns", int(df_metadata.shape[0]), schema=schema_name)
                telemetry.set_gauge("input_tables", int(df_metadata["TABLE_NAME"].nunique()), schema=schema_name)
                telemetry.increment("tables_read", int(df_metadata["TABLE_NAME"].nunique()), schema=schema_name)

            metadata_validator = MetadataValidator(
                df=df_metadata,
                table_plural_exceptions=options.plural_table_exceptions,
                config=options.validation_config or ValidationConfig(),
                schema_name=schema_name,
            )

            with (telemetry.stage("metadata.annotate_candidates", schema=schema_name) if telemetry is not None else nullcontext()):
                annotated_metadata = metadata_validator.annotate_data_quality_candidates(df_metadata)

            candidates_df = annotated_metadata.loc[
                annotated_metadata["FORMAT_CONFORMITY_CANDIDATE"].astype(bool)
                | annotated_metadata["REDUNDANCY_CANDIDATE"].astype(bool)
            ].copy()
            if telemetry is not None:
                telemetry.set_gauge("candidates_total", int(candidates_df.shape[0]), schema=schema_name)
                telemetry.increment("candidates_generated", int(candidates_df.shape[0]), schema=schema_name)
                if not candidates_df.empty:
                    telemetry.set_gauge(
                        "format_candidates",
                        int(candidates_df["FORMAT_CONFORMITY_CANDIDATE"].astype(bool).sum()),
                        schema=schema_name,
                    )
                    telemetry.set_gauge(
                        "redundancy_candidates",
                        int(candidates_df["REDUNDANCY_CANDIDATE"].astype(bool).sum()),
                        schema=schema_name,
                    )

            with (telemetry.stage("samples.load", schema=schema_name) if telemetry is not None else nullcontext()):
                samples_by_table = sample_source.get_samples_for_schema(schema_name, candidates_df)

            if telemetry is not None:
                telemetry.set_gauge("sample_tables_loaded", len(samples_by_table), schema=schema_name)

            sections = dq_validator.validate_candidates(
                schema_name=schema_name,
                candidates_df=candidates_df,
                samples_by_table=samples_by_table,
            )
            if telemetry is not None:
                telemetry.set_gauge("data_quality_metrics_rows", int(sections["DATA_QUALITY_METRICS"].shape[0]), schema=schema_name)
                telemetry.set_gauge("data_quality_issue_rows", int(sections["DATA_QUALITY_ISSUES"].shape[0]), schema=schema_name)

            with (telemetry.stage("excel.export", schema=schema_name) if telemetry is not None else nullcontext()):
                out_path = save_excel_report(
                    options.metadata_base_folder,
                    schema_name,
                    sections,
                    file_prefix="issues_dados",
                )
            print(f"Data quality report saved to {out_path}")


def _build_sample_source(options: RunDataQualityOptions) -> SampleSource:
    source_type = str(options.sample_source_type).strip().lower()
    if source_type == "csv":
        if options.sample_base_folder is None:
            raise ValueError("sample_base_folder is required when sample_source_type='csv'")
        return CsvSampleSource(Path(options.sample_base_folder))
    if source_type == "database":
        if not options.db_connection_uri:
            raise ValueError("db_connection_uri is required when sample_source_type='database'")
        return DatabaseSampleSource(
            connection_uri=options.db_connection_uri,
            db_type=options.db_type,
            authentication_type=options.db_authentication_type,
            driver_class_name=options.db_driver_class_name,
            sample_limit=options.sample_limit,
            query_template=options.sample_query_template,
        )
    raise ValueError(f"Unsupported sample_source_type: {options.sample_source_type}")


def _parse_exclude_tables(items: List[str]) -> list[tuple[str | None, str]]:
    result: list[tuple[str | None, str]] = []
    for raw in items:
        if not raw:
            continue
        text = str(raw).strip()
        if not text:
            continue
        if "." in text:
            owner, table = text.split(".", 1)
            owner = owner.strip().upper()
            table = table.strip().upper()
            if owner == "*":
                owner = None
            if table:
                result.append((owner, table))
        else:
            result.append((None, text.upper()))
    return result


def _filter_excluded_tables(df: pd.DataFrame, exclude_set: list[tuple[str | None, str]]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "OWNER" not in df.columns or "TABLE_NAME" not in df.columns:
        return df

    owners = df["OWNER"].astype(str).str.upper()
    tables = df["TABLE_NAME"].astype(str).str.upper()
    mask_exclude = pd.Series(False, index=df.index)

    for owner, pattern in exclude_set:
        owner_mask = owners == owner if owner else pd.Series(True, index=df.index)
        table_mask = tables.str.contains(pattern, na=False, regex=False)
        mask_exclude |= owner_mask & table_mask

    return df.loc[~mask_exclude].copy()
