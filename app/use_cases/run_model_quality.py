from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dataquality.domain.config.llm_comment_config import LLMCommentConfig
from dataquality.domain.config.validation_config import ValidationConfig
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.infrastructure.io.csv.schema_loader import schemaLoader
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


def run_model_quality(options: RunOptions) -> None:
    """End-to-end runner for the *model quality* phase (schema metadata validation + metrics)."""
    
    #print("\nSummary:")
    telemetry = get_current_telemetry()

    with (telemetry.stage("metadata.load") if telemetry is not None else nullcontext()):
        loader = schemaLoader(Path(options.base_folder), options.columns_to_delete)
        dfs = loader.get_dictionary()

    print(f"Total dataframes loaded: {len(dfs)}")
    print(f"Dictionary keys: {list(dfs.keys())}")
    if telemetry is not None:
        telemetry.set_metadata(use_case="run_model_quality")
        telemetry.set_gauge("schemas_loaded", len(dfs))

    exclude_set = _parse_exclude_tables(options.exclude_tables or [])

    for schema_name, df in dfs.items():
        if schema_name != "cadastro":  # --- IGNORE FOR TESTS---
            continue                     # --- IGNORE ---
        
        with (telemetry.stage("schema.process", schema=schema_name) if telemetry is not None else nullcontext()):
            if exclude_set:
                df = _filter_excluded_tables(df, exclude_set)

            df_schema_metadata = df.copy() # preserve original for the Excel first sheet

            print("\n==============================")
            print(f"Validating schema: {schema_name}")
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
                )
                sections = metadata_calculator.calculate_sections()

            if telemetry is not None:
                telemetry.set_gauge("candidates_total", int(sections["DATA_QUALITY_RULE_CANDIDATES"].shape[0]), schema=schema_name)

            with (telemetry.stage("excel.export", schema=schema_name) if telemetry is not None else nullcontext()):
                out_path = save_excel_report(options.base_folder, schema_name, sections)

            print(f"Issues saved to {out_path}")


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
