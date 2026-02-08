from __future__ import annotations

import pandas as pd

from dataquality.shared.utils import safe_iqmd
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.adapters.outbound.exporters.excel_report import build_section_df


class MetadataQualityMetricsCalculator:
    def __init__(self, schema_name: str, validator: MetadataValidator, df_schema_metadata: pd.DataFrame | None = None):
        self.schema_name = schema_name
        self.validator = validator
        self.df_schema_metadata = df_schema_metadata

    def calculate_sections(self) -> dict[str, pd.DataFrame]:
        """
        Returns DataFrames for metadata quality sections.
        - 0_SCHEMA_METADATA: raw input
        - schema_MEASURES: totals for schema (tables, columns, keys)
        - METADATA_ISSUES: validator.issues_df with standard columns
        - METADATA_MEASURES: totals for metadata scope
        - METADATA_METRICS: quality indicators based on issues/denominators
        """

        df_schema = (self.df_schema_metadata.copy() if self.df_schema_metadata is not None else pd.DataFrame())
        df_issues = self.validator.issues_df.copy()

        schema_specs = [
            ("MQME001", "Total number of tables", self.validator.get_number_tables),
            ("MQME002", "Total number of columns", self.validator.get_number_columns),
            ("MQME003", "Total number of primary key", self.validator.get_number_primary_keys),
            ("MQME004", "Total number of foreign key", self.validator.get_number_foreign_keys),
            ("MQME005", "Total number of unique key", self.validator.get_number_unique_keys),
        ]

        mq = {code: fn() for code, _, fn in schema_specs}
        df_schema = build_section_df([(code, desc, mq[code]) for code, desc, _ in schema_specs])

        rows_by_table = self.validator.get_rows_by_table()

        measures_specs = [
            ("MQME006", "Total number of length-required columns", self.validator.get_number_length_required),
            ("MQME007", "Total number of NUMBER columns", self.validator.get_number_number_types),
            ("MQME017", "Total number of rows in schema", self.validator.get_total_rows_schema),
            ("MQME018", "Total number of cells in schema", self.validator.get_total_cells_schema),
            ("MQME019", "Total number of null values (nullable, no default) in schema", self.validator.get_num_nulls_nullable_without_default),
        ]
        mq.update({code: fn() for code, _, fn in measures_specs})

        measure_rows = [
            (code, desc, mq[code]) for code, desc, _ in measures_specs
        ]
        if not rows_by_table.empty:
            for table_name, row_count in rows_by_table.items():
                measure_rows.append(
                    ("MQME007", f"Total rows for table {table_name}", int(row_count))
                )
        else:
            measure_rows.append(("MQME007", "Total rows for table (missing input column)", 0))

        df_measures = build_section_df(measure_rows)

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
            ("MQID001", "Table names in singular", ("MQME012", "MQME001")),
            ("MQID002", "Table with recommended name length", ("MQME013", "MQME001")),
            ("MQID003", "Columns with correct prefixes", ("MQME014", "MQME002")),
            ("MQID004", "Columns with recommended name size", ("MQME015", "MQME002")),
            ("MQID005", "Columns with comments", ("MQME008", "MQME002")),
            ("MQID006", "Table with standard PK prefixes", ("MQME009", "MQME003")),
            ("MQID007", "Table with standard FK prefixes", ("MQME010", "MQME004")),
            ("MQID008", "Table with standard UK prefixes", ("MQME011", "MQME005")),
            ("MQID010", "Columns with valid num_distinct", ("MQME021", "MQME002")),
            ("MQID011", "Columns with num_nulls", ("MQME019", "MQME018")),
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
            "schema_MEASURES": df_schema,
            "METADATA_ISSUES": df_issues,
            "METADATA_MEASURES": df_measures,
            "METADATA_METRICS": df_metrics,
        }
