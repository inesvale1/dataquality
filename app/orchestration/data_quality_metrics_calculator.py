from __future__ import annotations

from typing import Dict

import pandas as pd

from dataquality.shared.utils import safe_iqmd
from dataquality.domain.validators.data_model_validator import DataModelValidator
from dataquality.adapters.outbound.exporters.excel_report import build_section_df


class DataQualityMetricsCalculator:
    def __init__(self, scheme_name: str, validator: DataModelValidator | None = None, df_schema_metadata: pd.DataFrame | None = None):
      self.scheme_name          = scheme_name
      self.validator            = validator
      self.df_schema_metadata   = df_schema_metadata
      
    def calculate_sections(self) -> dict[str, pd.DataFrame]:
        """
        Returns DataFrames for each worksheet section.
        - 0_SCHEMA_METADATA: content of the input CSV, complete (raw input)
        - 1_ISSUES: validator.issues_df with columns: rule, desc, owner, table, column, constraint_name, length, limit
        - 2_SCHEME_MEASURES: specific measurements for each table.
        - 3_MODEL_MEASURES: specific measurements for each model, in relation to the defined standards.
        - 4_MODEL_INDICATORS: calculated percentages of issues found in relation to the model measures
        """

        # -------------------------
        # 0) SCHEMA METADATA (raw input)
        # -------------------------
        df_schema = (self.df_schema_metadata.copy() if self.df_schema_metadata is not None else pd.DataFrame())

        # -------------------------
        # 1) ISSUES sheet
        # -------------------------
        # Example assumes issues_df has columns: rule, desc, ...
        df_issues = self.validator.issues_df.copy()

        # -------------------------
        # 2) SCHEME MEASURES specs
        # -------------------------
        scheme_specs = [
            ("MQMD01", "Total number of tables", self.validator.get_number_tables),
            ("MQMD02", "Total number of columns", self.validator.get_number_columns),
            ("MQMD03", "Total number of primary key", self.validator.get_number_primary_keys),
            ("MQMD04", "Total number of foreign key", self.validator.get_number_foreign_keys),
            ("MQMD05", "Total number of unique key", self.validator.get_number_unique_keys),
        ]

        mqmd = {code: fn() for code, _, fn in scheme_specs}

        df_scheme = build_section_df(
            [(code, desc, mqmd[code]) for code, desc, _ in scheme_specs]
        )

        # ---------------------------------------
        # 3) MODEL MEASURES from validator.issues_df
        # ---------------------------------------
        df_count = (
            self.validator.issues_df
            .groupby("rule")
            .agg(
                Value=("rule", "size"),
                Description=("desc", "first")
            )
            .reset_index()
            .rename(columns={"rule": "Indicator"})
            .sort_values(by="Indicator", ascending=True)
        )

        df_measures = df_count[["Indicator", "Description", "Value"]].copy()

        # Make MQMD06..MQMD13 available for indicators:
        # (counts keyed by rule name)
        for _, row in df_measures.iterrows():
            mqmd[row["Indicator"]] = int(row["Value"])

        # -------------------------
        # 4) MODEL INDICATORS specs
        # -------------------------
        indicator_specs = [
            ("IQMD01", "Table names in singular", ("MQMD06", "MQMD01")),
            ("IQMD02", "Table names with recommended size", ("MQMD07", "MQMD01")),
            ("IQMD03", "Table columns with correct prefixes", ("MQMD08", "MQMD02")),
            ("IQMD04", "Table columns with recommend size", ("MQMD09", "MQMD02")),
            ("IQMD05", "Table columns with comments", ("MQMD10", "MQMD02")),
            ("IQMD06", "Table with standard PK prefixes", ("MQMD11", "MQMD03")),
            ("IQMD07", "Table with standard FK prefixes", ("MQMD12", "MQMD04")),
            ("IQMD08", "Table with standard UK prefixes", ("MQMD13", "MQMD05")),
        ]

        df_metrics_rows = []
        for code, desc, (num_code, den_code) in indicator_specs:
            num = float(mqmd.get(num_code, 0))
            den = float(mqmd.get(den_code, 0))
            value = safe_iqmd(num, den)
            df_metrics_rows.append((code, desc, f"{value:.2f}%"))

        df_metrics = build_section_df(df_metrics_rows)

        return {
            "0_SCHEMA_METADATA": df_schema,
            "1_ISSUES": df_issues,
            "2_SCHEME_MEASURES": df_scheme,
            "3_MODEL_MEASURES": df_measures,
            "4_MODEL_METRICS": df_metrics,
        }
