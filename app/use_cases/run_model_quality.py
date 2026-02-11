from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dataquality.domain.config.validation_config import ValidationConfig
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.infrastructure.io.csv.schema_loader import schemaLoader
from dataquality.app.orchestration.metadata_quality_metrics_calculator import MetadataQualityMetricsCalculator
from dataquality.adapters.outbound.exporters.excel_report import save_excel_report

import pandas as pd

@dataclass
class RunOptions:
    base_folder: Path
    columns_to_delete: List[str]
    plural_table_exceptions: List[str]
    validation_config: Optional[ValidationConfig] = None
    db_type: str = "Oracle"
    exclude_tables: List[str] | None = None


def run_model_quality(options: RunOptions) -> None:
    """End-to-end runner for the *model quality* phase (schema metadata validation + metrics)."""
    
    print("\nSummary:")
       
    loader = schemaLoader(Path(options.base_folder), options.columns_to_delete)
    dfs = loader.get_dictionary()

    print(f"Total dataframes loaded: {len(dfs)}")
    print(f"Dictionary keys: {list(dfs.keys())}")

    exclude_set = _parse_exclude_tables(options.exclude_tables or [])

    for schema_name, df in dfs.items():
        #if schema_name != "cadastro":  # --- IGNORE FOR TESTS---
        #    continue                     # --- IGNORE ---
        
        if exclude_set:
            df = _filter_excluded_tables(df, exclude_set)

        df_schema_metadata = df.copy() # preserve original for the Excel first sheet

        print("\n==============================")
        print(f"Validating schema: {schema_name}")
        print("==============================")
        
        validator = MetadataValidator(
            df=df,
            table_plural_exceptions=options.plural_table_exceptions,
            config=options.validation_config or ValidationConfig(),
        )

        issues = validator.run_all()
        if issues.empty:
            print("\n--- No Issue found ---")
            continue

        metadata_calculator = MetadataQualityMetricsCalculator(
            schema_name=schema_name,
            validator=validator,
            df_schema_metadata=df_schema_metadata,
            db_type=options.db_type,
        )
        sections = metadata_calculator.calculate_sections()

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

