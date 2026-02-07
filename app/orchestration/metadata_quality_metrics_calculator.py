from __future__ import annotations

import pandas as pd

from dataquality.shared.utils import safe_iqmd
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.adapters.outbound.exporters.excel_report import build_section_df


class MetadataQualityMetricsCalculator:
    def __init__(self, scheme_name: str, validator: MetadataValidator, df_schema_metadata: pd.DataFrame | None = None):
        self.scheme_name = scheme_name
        self.validator = validator
        self.df_schema_metadata = df_schema_metadata

    def calculate_sections(self) -> dict[str, pd.DataFrame]:
        """
        Returns DataFrames for metadata quality sections.
        - 0_SCHEMA_METADATA: raw input
        - SCHEME_MEASURES: totals for schema (tables, columns, keys)
        - METADATA_ISSUES: validator.issues_df with standard columns
        - METADATA_MEASURES: totals for metadata scope
        - METADATA_METRICS: quality indicators based on issues/denominators
        """

        df_schema = (self.df_schema_metadata.copy() if self.df_schema_metadata is not None else pd.DataFrame())
        df_issues = self.validator.issues_df.copy()

        scheme_specs = [
            ("MQME01", "Total number of tables", self.validator.get_number_tables),
            ("MQME02", "Total number of columns", self.validator.get_number_columns),
            ("MQME03", "Total number of primary key", self.validator.get_number_primary_keys),
            ("MQME04", "Total number of foreign key", self.validator.get_number_foreign_keys),
            ("MQME05", "Total number of unique key", self.validator.get_number_unique_keys),
        ]

        mq = {code: fn() for code, _, fn in scheme_specs}
        df_scheme = build_section_df([(code, desc, mq[code]) for code, desc, _ in scheme_specs])

        measures_specs = [
            ("MQME06", "Total number of length-required columns", self.validator.get_number_length_required),
            ("MQME07", "Total number of NUMBER columns", self.validator.get_number_number_types),
        ]
        mq.update({code: fn() for code, _, fn in measures_specs})

        df_measures = build_section_df([(code, desc, mq[code]) for code, desc, _ in measures_specs])

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

        for _, row in df_count.iterrows():
            mq[row["Indicator"]] = int(row["Value"])

        indicator_specs = [
            ("MQID01", "Table names in singular", ("MQME20", "MQME01")),
            ("MQID02", "Table names with recommended size", ("MQME21", "MQME01")),
            ("MQID03", "Table columns with correct prefixes", ("MQME22", "MQME02")),
            ("MQID04", "Table columns with recommended size", ("MQME23", "MQME02")),
            ("MQID05", "Table columns with comments", ("MQME10", "MQME02")),
            ("MQID06", "Table with standard PK prefixes", ("MQME24", "MQME03")),
            ("MQID07", "Table with standard FK prefixes", ("MQME25", "MQME04")),
            ("MQID08", "Table with standard UK prefixes", ("MQME26", "MQME05")),
            ("MQID09", "Columns with data type", ("MQME11", "MQME02")),
            ("MQID10", "Length-required columns with data length", ("MQME12", "MQME06")),
            ("MQID11", "NUMBER columns with valid scale", ("MQME13", "MQME07")),
            ("MQID12", "Columns with valid num_distinct", ("MQME14", "MQME02")),
            ("MQID13", "Columns with valid num_nulls", ("MQME15", "MQME02")),
            ("MQID14", "Columns with valid density", ("MQME16", "MQME02")),
        ]

        df_metrics_rows = []
        for code, desc, (num_code, den_code) in indicator_specs:
            num = float(mq.get(num_code, 0))
            den = float(mq.get(den_code, 0))
            value = safe_iqmd(num, den)
            df_metrics_rows.append((code, desc, f"{value:.2f}%"))

        df_metrics = build_section_df(df_metrics_rows)

        return {
            "0_SCHEMA_METADATA": df_schema,
            "SCHEME_MEASURES": df_scheme,
            "METADATA_ISSUES": df_issues,
            "METADATA_MEASURES": df_measures,
            "METADATA_METRICS": df_metrics,
        }
