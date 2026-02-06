from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dataquality.domain.config.validation_config import ValidationConfig
from dataquality.domain.validators.data_model_validator import DataModelValidator
from dataquality.infrastructure.io.csv.scheme_loader import SchemeLoader
from dataquality.app.orchestration.data_quality_metrics_calculator import DataQualityMetricsCalculator
from dataquality.adapters.outbound.exporters.excel_report import save_excel_report

import os

@dataclass
class RunOptions:
    base_folder: Path
    columns_to_delete: List[str]
    plural_table_exceptions: List[str]
    validation_config: Optional[ValidationConfig] = None


def run_model_quality(options: RunOptions) -> None:
    """End-to-end runner for the *model quality* phase (schema metadata validation + metrics)."""
    
    print("\nSummary:")
       
    loader = SchemeLoader(Path(options.base_folder), options.columns_to_delete)
    dfs = loader.get_dictionary()

    print(f"Total dataframes loaded: {len(dfs)}")
    print(f"Dictionary keys: {list(dfs.keys())}")

    for scheme_name, df in dfs.items():
        #if scheme_name != "cadastro":  # --- IGNORE FOR TESTS---
        #    continue                     # --- IGNORE ---
        
        df_schema_metadata = df.copy() # preserve original for the Excel first sheet

        print("\n==============================")
        print(f"Validating scheme: {scheme_name}")
        print("==============================")
        
        validator = DataModelValidator(
            df=df,
            table_plural_exceptions=options.plural_table_exceptions,
            config=options.validation_config or ValidationConfig(),
        )

        issues = validator.run_all()

        if issues.empty:
            print("\n--- No Issue found ---")
            continue

        calculator = DataQualityMetricsCalculator(
                        scheme_name=scheme_name,
                        validator=validator,
                        df_schema_metadata=df_schema_metadata
        )
        sections = calculator.calculate_sections()

        out_path = save_excel_report(options.base_folder, scheme_name, sections)

        print(f"Issues saved to {out_path}")

