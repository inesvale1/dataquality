from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dataquality.adapters.outbound.exporters.excel_report import save_excel_report
from dataquality.app.orchestration.document_code_quality_analyzer import DocumentCodeQualityAnalyzer
from dataquality.domain.config.scoring_config import ScoringConfig
from dataquality.domain.config.validation_config import ValidationConfig
from dataquality.domain.scoring.quality_scorer import (
    build_ddq_scores_df,
    build_schema_score_rows,
    compute_ddq,
    compute_schema_score,
)
from dataquality.shared.utils import safe_iqmd
from dataquality.domain.validators.data_quality_validator import DataQualityValidator
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.infrastructure.io.metadata_sources import build_metadata_source
from dataquality.infrastructure.io.sample_sources import CsvSampleSource, DatabaseSampleSource, S3SampleSource, SampleSource
from dataquality.infrastructure.io.secure_credentials import DatabaseConnectionSettings
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
    metadata_source_type: str = "csv"
    sample_source_type: str = "csv"
    db_connection_uri: str | None = None
    db_authentication_type: str = "username_password"
    db_driver_class_name: str | None = None
    db_username: str | None = None
    db_host: str | None = None
    db_port: int | None = None
    db_service_name: str | None = None
    db_sid: str | None = None
    db_dsn: str | None = None
    db_password_keyring_service: str | None = None
    db_password_keyring_username: str | None = None
    metadata_db_schemas: List[str] | None = None
    metadata_query_template: str | None = None
    metadata_s3_uri: str | None = None
    sample_s3_uri: str | None = None
    s3_storage_options: dict[str, object] | None = None
    sample_query_template: str | None = None
    sample_limit: int = 1000
    include_schemas: List[str] | None = None
    skip_document_code_analysis: bool = False
    scoring_config: ScoringConfig | None = None
    mddq_by_schema: dict[str, float | None] | None = None


def run_data_quality(options: RunDataQualityOptions) -> dict[str, float | None]:
    """End-to-end runner for the data quality phase.

    Returns a mapping of schema_name → DDQ score (0-100) for each processed schema.
    """
    print("\nSummary:")
    telemetry = get_current_telemetry()
    scoring_config = options.scoring_config or ScoringConfig()
    mddq_by_schema = options.mddq_by_schema or {}

    with (telemetry.stage("metadata.load") if telemetry is not None else nullcontext()):
        metadata_source = _build_metadata_source(options)
        metadata_by_schema = metadata_source.get_metadata_by_schema()

    metadata_by_schema = _filter_schemas(metadata_by_schema, options.include_schemas)
    sample_source = _build_sample_source(options)

    exclude_set = _parse_exclude_tables(options.exclude_tables or [])

    print(f"Total metadata schemas loaded: {len(metadata_by_schema)}")
    print(f"Sample source type: {options.sample_source_type}")
    if telemetry is not None:
        telemetry.set_metadata(
            use_case="run_data_quality",
            metadata_source_type=options.metadata_source_type,
            sample_source_type=options.sample_source_type,
            sample_limit=options.sample_limit,
        )
        telemetry.set_gauge("schemas_loaded", len(metadata_by_schema))

    dq_validator = DataQualityValidator()
    ddq_by_schema: dict[str, float | None] = {}

    for schema_name, df_metadata in metadata_by_schema.items():
        print("\n==============================")
        print(f"Validating data schema: {schema_name}")
        print("==============================")

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

            if candidates_df.empty:
                samples_by_table = {}
            else:
                try:
                    with (telemetry.stage("samples.load", schema=schema_name) if telemetry is not None else nullcontext()):
                        samples_by_table = sample_source.get_samples_for_schema(schema_name, candidates_df)
                except FileNotFoundError as exc:
                    print(f"[data_quality] Samples not available for schema '{schema_name}': {exc}. Skipping sample-based validation.")
                    samples_by_table = {}

            if telemetry is not None:
                telemetry.set_gauge("sample_tables_loaded", len(samples_by_table), schema=schema_name)

            sections = dq_validator.validate_candidates(
                schema_name=schema_name,
                candidates_df=candidates_df,
                samples_by_table=samples_by_table,
            )

            if not options.skip_document_code_analysis:
                with (telemetry.stage("document_codes.analyze", schema=schema_name) if telemetry is not None else nullcontext()):
                    doc_analyzer = DocumentCodeQualityAnalyzer(Path(options.metadata_base_folder))
                    doc_result = doc_analyzer.analyze_schema(schema_name)
                sections["DATA_ISSUES"] = doc_result.issues_df
                valid_codes = doc_result.total_distinct_codes - doc_result.invalid_or_ambiguous_distinct_codes
                mqid015_row = {
                    "Schema": schema_name,
                    "Owner": schema_name.upper(),
                    "Table": "",
                    "Column": "",
                    "Metric": "MQID015",
                    "Dimension": "Conformity",
                    "SemanticTag": "CPF/CNPJ/CGF",
                    "Priority": "HIGH",
                    "RuleType": "DOCUMENT_CODE_CONFORMITY",
                    "ExpectedFormat": "",
                    "CalculationMethod": "DATA_SCAN",
                    "EvaluatedRows": doc_result.total_distinct_codes,
                    "ValidRows": valid_codes,
                    "InvalidRows": doc_result.invalid_or_ambiguous_distinct_codes,
                    "Value": f"{safe_iqmd(doc_result.invalid_or_ambiguous_distinct_codes, doc_result.total_distinct_codes):.2f}",
                    "Status": "CALCULATED",
                }
                sections["DATA_QUALITY_METRICS"] = pd.concat(
                    [sections["DATA_QUALITY_METRICS"], pd.DataFrame([mqid015_row])],
                    ignore_index=True,
                )
                if telemetry is not None:
                    telemetry.set_gauge("document_code_total", doc_result.total_distinct_codes, schema=schema_name)
                    telemetry.set_gauge("document_code_invalid", doc_result.invalid_or_ambiguous_distinct_codes, schema=schema_name)

            sections.pop("DATA_QUALITY_RULE_CANDIDATES", None)

            # Granular per-column metrics — used for DDQ computation
            df_dq_metrics_granular = sections.get("DATA_QUALITY_METRICS", pd.DataFrame())
            ddq = compute_ddq(df_dq_metrics_granular, scoring_config.data_quality_metric_weights)
            ddq_by_schema[schema_name] = ddq

            df_quality_scores = build_ddq_scores_df(
                df_dq_metrics_granular, scoring_config.data_quality_metric_weights, ddq
            )
            mddq = mddq_by_schema.get(schema_name)
            if mddq is not None or ddq is not None:
                schema_score = compute_schema_score(mddq, ddq, scoring_config)
                extra_rows = build_schema_score_rows(mddq, ddq, schema_score, scoring_config)
                df_quality_scores = pd.concat(
                    [df_quality_scores, pd.DataFrame(extra_rows)], ignore_index=True
                )
                if schema_score is not None:
                    print(f"Schema score ({schema_name}): {schema_score:.2f}")
            sections["QUALITY_SCORES"] = df_quality_scores

            # Replace granular metrics with aggregated view (same format as METADATA_QUALITY_METRICS)
            sections["DATA_QUALITY_METRICS"] = _aggregate_dq_metrics(df_dq_metrics_granular)

            if ddq is not None:
                print(f"DDQ ({schema_name}): {ddq:.2f}")

            if telemetry is not None:
                telemetry.set_gauge("data_quality_metrics_rows", int(df_dq_metrics_granular.shape[0]), schema=schema_name)
                telemetry.set_gauge("data_quality_issue_rows", int(sections["DATA_QUALITY_ISSUES"].shape[0]), schema=schema_name)
                if ddq is not None:
                    telemetry.set_gauge("ddq", round(ddq, 4), schema=schema_name)

            with (telemetry.stage("excel.export", schema=schema_name) if telemetry is not None else nullcontext()):
                out_path = save_excel_report(
                    options.metadata_base_folder,
                    schema_name,
                    sections,
                    file_prefix="issues_dados",
                )
            print(f"Data quality report saved to {out_path}")

    return ddq_by_schema


_DQ_METRIC_DESCRIPTIONS: dict[str, str] = {
    "Format Conformity": "Format conformity across candidate columns",
    "Uniqueness": "Uniqueness across candidate columns",
    "Redundancy detection": "Redundancy detection across candidate columns",
    "MQID015": "Valid CPF/CNPJ/CGF",
}


def _aggregate_dq_metrics(df_granular: pd.DataFrame) -> pd.DataFrame:
    """Collapse per-column metric rows into one row per metric type.

    Output matches the METADATA_QUALITY_METRICS column layout:
    Indicator | Dimension | Description | Value
    """
    cols = ["Indicator", "Dimension", "Description", "Value"]
    if df_granular.empty:
        return pd.DataFrame(columns=cols)

    calculated = df_granular[
        df_granular["Status"].astype(str).str.startswith("CALCULATED", na=False)
    ].copy()
    if calculated.empty:
        return pd.DataFrame(columns=cols)

    calculated["_v"] = pd.to_numeric(calculated["Value"], errors="coerce")
    grouped = (
        calculated.groupby("Metric", sort=False)
        .agg(Dimension=("Dimension", "first"), _avg=("_v", "mean"))
        .reset_index()
        .rename(columns={"Metric": "Indicator"})
    )
    grouped["Description"] = grouped["Indicator"].map(_DQ_METRIC_DESCRIPTIONS).fillna(grouped["Indicator"])
    grouped["Value"] = grouped["_avg"].map(lambda v: f"{v:.2f}" if pd.notna(v) else "N/A")
    return grouped[cols]


def _filter_schemas(dfs: dict, include_schemas: List[str] | None) -> dict:
    if not include_schemas:
        return dfs
    allowed = {s.strip().lower() for s in include_schemas if s.strip()}
    return {k: v for k, v in dfs.items() if k.lower() in allowed}


def _build_sample_source(options: RunDataQualityOptions) -> SampleSource:
    source_type = str(options.sample_source_type).strip().lower()
    if source_type == "csv":
        if options.sample_base_folder is None:
            raise ValueError("sample_base_folder is required when sample_source_type='csv'")
        return CsvSampleSource(Path(options.sample_base_folder))
    if source_type in {"database", "db", "oracle"}:
        return DatabaseSampleSource(
            connection_settings=_build_connection_settings(options),
            db_type=options.db_type,
            authentication_type=options.db_authentication_type,
            driver_class_name=options.db_driver_class_name,
            sample_limit=options.sample_limit,
            query_template=options.sample_query_template,
        )
    if source_type == "s3":
        return S3SampleSource(str(options.sample_s3_uri or ""), options.s3_storage_options)
    raise ValueError(f"Unsupported sample_source_type: {options.sample_source_type}")


def _build_metadata_source(options: RunDataQualityOptions):
    return build_metadata_source(
        source_type=options.metadata_source_type,
        base_folder=Path(options.metadata_base_folder),
        columns_to_delete=options.columns_to_delete,
        connection_settings=_build_connection_settings(options),
        schemas=options.metadata_db_schemas,
        db_type=options.db_type,
        query_template=options.metadata_query_template,
        s3_uri=options.metadata_s3_uri,
        s3_storage_options=options.s3_storage_options,
    )


def _build_connection_settings(options: RunDataQualityOptions) -> DatabaseConnectionSettings:
    return DatabaseConnectionSettings(
        connection_uri=options.db_connection_uri,
        driver_class_name=options.db_driver_class_name,
        username=options.db_username,
        host=options.db_host,
        port=options.db_port,
        service_name=options.db_service_name,
        sid=options.db_sid,
        dsn=options.db_dsn,
        password_keyring_service=options.db_password_keyring_service,
        password_keyring_username=options.db_password_keyring_username,
    )


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
