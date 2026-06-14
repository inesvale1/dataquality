from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dataquality.domain.config.llm_comment_config import LLMCommentConfig
from dataquality.domain.config.scoring_config import ScoringConfig
from dataquality.domain.config.validation_config import ValidationConfig
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.infrastructure.io.metadata_sources import build_metadata_source
from dataquality.infrastructure.io.secure_credentials import DatabaseConnectionSettings
from dataquality.infrastructure.io.oracle.result_exporter import OracleResultExporter, build_oracle_exporter
from dataquality.app.orchestration.metadata_quality_metrics_calculator import MetadataQualityMetricsCalculator
from dataquality.adapters.outbound.exporters.excel_report import save_excel_report
from dataquality.shared.telemetry import get_current_telemetry

import pandas as pd

@dataclass
class RunOptions:
    base_folder: Path
    columns_to_delete: List[str]
    plural_table_exceptions: List[str]
    validation_config: Optional[ValidationConfig] = None
    db_type: str = "Oracle"
    exclude_tables: List[str] | None = None
    llm_comment_config: LLMCommentConfig | None = None
    context_output_dir: Path | None = None
    save_context_json: bool = True
    metadata_source_type: str = "csv"
    db_connection_uri: str | None = None
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
    metadata_query_file: str | None = None
    metadata_s3_uri: str | None = None
    s3_storage_options: dict[str, object] | None = None
    include_schemas: List[str] | None = None
    regenerate_context: bool = True
    scoring_config: ScoringConfig | None = None
    # output: "excel" | "oracle" | "both"
    output_type: str = "excel"


def run_model_quality(options: RunOptions) -> dict[str, float | None]:
    """End-to-end runner for the *model quality* phase (schema metadata validation + metrics).

    Returns a mapping of schema_name → MDDQ score (0-100) for each processed schema.
    """
    telemetry = get_current_telemetry()

    with (telemetry.stage("metadata.load") if telemetry is not None else nullcontext()):
        metadata_source = build_metadata_source(
            source_type=options.metadata_source_type,
            base_folder=Path(options.base_folder),
            columns_to_delete=options.columns_to_delete,
            connection_settings=_build_connection_settings(options),
            schemas=options.metadata_db_schemas,
            db_type=options.db_type,
            query_template=options.metadata_query_template,
            query_file=options.metadata_query_file,
            s3_uri=options.metadata_s3_uri,
            s3_storage_options=options.s3_storage_options,
        )
        dfs = metadata_source.get_metadata_by_schema()

    dfs = _filter_schemas(dfs, options.include_schemas)
    print(f"Total dataframes loaded: {len(dfs)}")
    print(f"Dictionary keys: {list(dfs.keys())}")
    if telemetry is not None:
        telemetry.set_metadata(use_case="run_model_quality", metadata_source_type=options.metadata_source_type)
        telemetry.set_gauge("schemas_loaded", len(dfs))

    exclude_set = _parse_exclude_tables(options.exclude_tables or [])
    mddq_by_schema: dict[str, float | None] = {}

    for schema_name, df in dfs.items():
        with (telemetry.stage("schema.process", schema=schema_name) if telemetry is not None else nullcontext()):
            if exclude_set:
                df = _filter_excluded_tables(df, exclude_set)

            df_schema_metadata = df.copy()

            print("\n==============================")
            print(f"Validating metadata schema: {schema_name}")
            print("==============================")
            if telemetry is not None:
                telemetry.set_gauge("input_columns", int(df.shape[0]), schema=schema_name)
                telemetry.set_gauge("input_tables", int(df["TABLE_NAME"].nunique()), schema=schema_name)
                telemetry.increment("tables_read", int(df["TABLE_NAME"].nunique()), schema=schema_name)

            validator = MetadataValidator(
                df=df,
                table_plural_exceptions=options.plural_table_exceptions,
                config=options.validation_config or ValidationConfig(),
                schema_name=schema_name,
            )

            issues = validator.run_all()
            if telemetry is not None:
                telemetry.set_gauge("metadata_issue_rows", int(issues.shape[0]), schema=schema_name)
            if issues.empty:
                print("\n--- No metadata issue found; generating report with metrics and data-quality candidates ---")

            with (telemetry.stage("metadata.metrics_calculation", schema=schema_name) if telemetry is not None else nullcontext()):
                metadata_calculator = MetadataQualityMetricsCalculator(
                    schema_name=schema_name,
                    validator=validator,
                    df_schema_metadata=df_schema_metadata,
                    db_type=options.db_type,
                    llm_comment_config=options.llm_comment_config,
                    context_output_dir=options.context_output_dir,
                    save_context_json=options.save_context_json,
                    base_folder=options.base_folder,
                    regenerate_context=options.regenerate_context,
                    scoring_config=options.scoring_config,
                )
                sections, mddq = metadata_calculator.calculate_sections()

            mddq_by_schema[schema_name] = mddq
            if mddq is not None:
                print(f"MDDQ ({schema_name}): {mddq:.2f}")

            if telemetry is not None:
                telemetry.set_gauge("candidates_total", int(sections["DATA_QUALITY_RULE_CANDIDATES"].shape[0]), schema=schema_name)
                if mddq is not None:
                    telemetry.set_gauge("mddq", round(mddq, 4), schema=schema_name)

            output_type = str(options.output_type or "excel").strip().lower()

            if output_type in {"excel", "both"}:
                with (telemetry.stage("excel.export", schema=schema_name) if telemetry is not None else nullcontext()):
                    out_path = save_excel_report(options.base_folder, schema_name, dict(sections))
                print(f"Issues saved to {out_path}")

            if output_type in {"oracle", "both"}:
                with (telemetry.stage("oracle.export", schema=schema_name) if telemetry is not None else nullcontext()):
                    exporter = build_oracle_exporter(_build_connection_settings(options))
                    exec_id = exporter.begin_execution(
                        owner=schema_name,
                        source_type=options.metadata_source_type.upper(),
                        notes=f"issues_metadados phase — MDDQ={mddq:.2f}" if mddq is not None else "issues_metadados phase",
                    )
                    try:
                        exporter.save_quality_scores(exec_id, sections["METADATA_SCORES"])
                        exporter.finish_execution(exec_id, "SUCCESS")
                    except Exception:
                        exporter.finish_execution(exec_id, "ERROR")
                        raise
                    finally:
                        exporter.dispose()
                print(f"[oracle] Results written to DQ_EXECUTION / DQ_QUALITY_SCORE (execution {exec_id})")

    return mddq_by_schema


def _filter_schemas(dfs: dict, include_schemas: List[str] | None) -> dict:
    if not include_schemas:
        return dfs
    allowed = {s.strip().lower() for s in include_schemas if s.strip()}
    return {k: v for k, v in dfs.items() if k.lower() in allowed}


def _parse_exclude_tables(items: List[str]) -> list[tuple[str, str | None]]:
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


def _filter_excluded_tables(df: "pd.DataFrame", exclude_set: list[tuple[str | None, str]]) -> "pd.DataFrame":
    if df is None or df.empty:
        return df
    if "OWNER" not in df.columns or "TABLE_NAME" not in df.columns:
        return df
    owners = df["OWNER"].astype(str).str.upper()
    tables = df["TABLE_NAME"].astype(str).str.upper()
    mask_exclude = pd.Series(False, index=df.index)
    for owner, pattern in exclude_set:
        if owner:
            owner_mask = owners == owner
        else:
            owner_mask = pd.Series(True, index=df.index)
        table_mask = tables.str.contains(pattern, na=False, regex=False)
        mask_exclude |= owner_mask & table_mask
    return df.loc[~mask_exclude].copy()


def _build_connection_settings(options: RunOptions) -> DatabaseConnectionSettings:
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
