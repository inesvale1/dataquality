from __future__ import annotations

import pandas as pd

from dataquality.shared.utils import safe_iqmd
from dataquality.domain.config.metadata_metric_config import METADATA_INDICATOR_SPECS
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.adapters.outbound.exporters.excel_report import build_section_df
from dataquality.domain.suggesters.metadata_issue_suggester import MetadataIssueSuggester


class MetadataQualityMetricsCalculator:
    def __init__(
        self,
        schema_name: str,
        validator: MetadataValidator,
        df_schema_metadata: pd.DataFrame | None = None,
        db_type: str = "Oracle",
    ):
        self.schema_name = schema_name
        self.validator = validator
        self.df_schema_metadata = df_schema_metadata
        self.db_type = db_type

    def calculate_sections(self) -> dict[str, pd.DataFrame]:
        """
        Returns DataFrames for metadata quality sections.
        - SCHEMA_METADATA: raw input
        - METADATA_MEASURES: totals for metadata scope (includes schema totals)
        - METADATA_ISSUES: validator.issues_df with standard columns
        - METADATA_METRICS: quality indicators (percentual) for the whole schema
        """

        df_schema_metadata = (self.df_schema_metadata.copy() if self.df_schema_metadata is not None else pd.DataFrame())
        df_schema_metadata = self.validator.annotate_data_quality_candidates(df_schema_metadata)
        df_data_quality_candidates = self._build_data_quality_candidates(df_schema_metadata)
        df_issues = self.validator.issues_df.copy()
        suggester = MetadataIssueSuggester(db_type=self.db_type)
        df_issues = suggester.apply(df_issues, df_schema_metadata)

        raw_measure_specs = [
            ("MQME001", "Total number of tables", self.validator.get_number_tables),
            ("MQME002", "Total number of columns", self.validator.get_number_columns),
            ("MQME003", "Total number of primary key", self.validator.get_number_primary_keys),
            ("MQME004", "Total number of foreign key", self.validator.get_number_foreign_keys),
            ("MQME005", "Total number of unique key", self.validator.get_number_unique_keys),
            ("MQME017", "Total number of rows in schema", self.validator.get_total_rows_schema),
            ("MQME018", "Total number of cells in schema (sum of columns x rows for each table)", self.validator.get_total_cells_schema),
            ("MQME019", "Total number of null values (nullable, no default) in schema", self.validator.get_num_nulls_nullable_without_default),
        ]

        derived_measure_specs = [
            ("MQME006", "Total number of length-required columns", self.validator.get_number_length_required),
            ("MQME007", "Total number of NUMBER columns", self.validator.get_number_number_types),
            ("MQME022", "Total number of tables without PK or UK", self.validator.get_number_tables_without_pk_or_uk),
            ("MQME023", "Total number of identifier-like columns", self.validator.get_number_identifier_like_columns),
            ("MQME024", "Total number of identifier-like columns without PK/FK/UK", self.validator.get_number_identifier_like_columns_without_protection),
            ("MQME025", "Total number of type/naming convention candidate columns", self.validator.get_number_type_naming_candidates),
            ("MQME026", "Total number of non-compliant type/naming convention columns", self.validator.get_number_type_naming_noncompliant_columns),
            ("MQME027", "Total number of tables without comments", self.validator.get_number_tables_without_comments),
        ]

        mq = {code: fn() for code, _, fn in raw_measure_specs}
        mq.update({code: fn() for code, _, fn in derived_measure_specs})

        raw_measure_rows = [(code, "RAW", desc, mq[code]) for code, desc, _ in raw_measure_specs]
        derived_measure_rows = [(code, "DERIVED", desc, mq[code]) for code, desc, _ in derived_measure_specs]

        rows_by_table = self.validator.get_rows_by_table()
        null_percent_by_table = self.validator.get_null_percent_by_table_nullable_without_default()

        measure_rows = raw_measure_rows + derived_measure_rows
        if not rows_by_table.empty:
            for table_name, row_count in rows_by_table.items():
                measure_rows.append(
                    ("MQME007", "RAW", f"Total rows for table {table_name}", int(row_count))
                )
        else:
            measure_rows.append(("MQME007", "RAW", "Total rows for table (missing input column)", 0))

        if not null_percent_by_table.empty:
            for table_name, null_percent in null_percent_by_table.items():
                measure_rows.append(
                    ("MQME019", "DERIVED", f"Percent null values for table {table_name}", f"{float(null_percent):.2f}")
                )

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

        df_metrics_rows = []
        for spec in METADATA_INDICATOR_SPECS:
            num = float(mq.get(spec.numerator_measure, 0))
            den = float(mq.get(spec.denominator_measure, 0))
            value = safe_iqmd(num, den)
            df_metrics_rows.append((spec.indicator, spec.dimension, spec.description, f"{value:.2f}"))

        df_metrics = pd.DataFrame(
            df_metrics_rows,
            columns=["Indicator", "Dimension", "Description", "Value"],
        )

        return {
            "SCHEMA_METADATA": df_schema_metadata,
            "DATA_QUALITY_RULE_CANDIDATES": df_data_quality_candidates,
            "METADATA_MEASURES": df_measures,
            "METADATA_ISSUES": df_issues,
            "METADATA_METRICS": df_metrics,
        }

    def _build_data_quality_candidates(self, df_schema_metadata: pd.DataFrame) -> pd.DataFrame:
        if df_schema_metadata.empty:
            return pd.DataFrame()

        candidate_frames: list[pd.DataFrame] = []

        if "FORMAT_CONFORMITY_CANDIDATE" in df_schema_metadata.columns:
            df_format = df_schema_metadata.loc[
                df_schema_metadata["FORMAT_CONFORMITY_CANDIDATE"].astype(bool),
                [
                    "OWNER",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "FORMAT_CONFORMITY_METRIC",
                    "FORMAT_CONFORMITY_DIMENSION",
                    "FORMAT_CONFORMITY_SEMANTIC_TAG",
                    "FORMAT_CONFORMITY_RULE_TYPE",
                    "FORMAT_CONFORMITY_EXPECTED_FORMAT",
                    "FORMAT_CONFORMITY_PRIORITY",
                    "FORMAT_CONFORMITY_DESCRIPTION",
                ],
            ].copy()
            df_format.columns = [
                "OWNER",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "METRIC",
                "DIMENSION",
                "SEMANTIC_TAG",
                "RULE_TYPE",
                "EXPECTED_FORMAT",
                "PRIORITY",
                "DESCRIPTION",
            ]
            df_format["CALCULATION_METHOD"] = "DATA_SCAN_REQUIRED"
            df_format["NUM_ROWS"] = df_schema_metadata.loc[df_format.index, "NUM_ROWS"].values if "NUM_ROWS" in df_schema_metadata.columns else ""
            df_format["NUM_NULLS"] = df_schema_metadata.loc[df_format.index, "NUM_NULLS"].values if "NUM_NULLS" in df_schema_metadata.columns else ""
            df_format["NUM_DISTINCT"] = df_schema_metadata.loc[df_format.index, "NUM_DISTINCT"].values if "NUM_DISTINCT" in df_schema_metadata.columns else ""
            candidate_frames.append(df_format)

        if "REDUNDANCY_CANDIDATE" in df_schema_metadata.columns:
            redundancy_columns = [
                "OWNER",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "REDUNDANCY_METRIC",
                "REDUNDANCY_DIMENSION",
                "REDUNDANCY_RULE_TYPE",
                "REDUNDANCY_PRIORITY",
                "REDUNDANCY_DESCRIPTION",
                "REDUNDANCY_CALCULATION_METHOD",
            ]
            df_redundancy_source = df_schema_metadata.loc[
                df_schema_metadata["REDUNDANCY_CANDIDATE"].astype(bool)
            ].copy()
            for col in redundancy_columns:
                if col not in df_redundancy_source.columns:
                    df_redundancy_source[col] = ""
            df_redundancy = df_redundancy_source[
                [
                    "OWNER",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "REDUNDANCY_METRIC",
                    "REDUNDANCY_DIMENSION",
                    "REDUNDANCY_RULE_TYPE",
                    "REDUNDANCY_PRIORITY",
                    "REDUNDANCY_DESCRIPTION",
                ]
            ].copy()
            df_redundancy["SEMANTIC_TAG"] = ""
            df_redundancy["EXPECTED_FORMAT"] = ""
            df_redundancy["REDUNDANCY_CALCULATION_METHOD"] = df_redundancy_source["REDUNDANCY_CALCULATION_METHOD"].values
            df_redundancy = df_redundancy[
                [
                    "OWNER",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "REDUNDANCY_METRIC",
                    "REDUNDANCY_DIMENSION",
                    "SEMANTIC_TAG",
                    "REDUNDANCY_RULE_TYPE",
                    "EXPECTED_FORMAT",
                    "REDUNDANCY_PRIORITY",
                    "REDUNDANCY_DESCRIPTION",
                    "REDUNDANCY_CALCULATION_METHOD",
                ]
            ]
            df_redundancy.columns = [
                "OWNER",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "METRIC",
                "DIMENSION",
                "SEMANTIC_TAG",
                "RULE_TYPE",
                "EXPECTED_FORMAT",
                "PRIORITY",
                "DESCRIPTION",
                "CALCULATION_METHOD",
            ]
            df_redundancy["NUM_ROWS"] = df_schema_metadata.loc[df_redundancy.index, "NUM_ROWS"].values if "NUM_ROWS" in df_schema_metadata.columns else ""
            df_redundancy["NUM_NULLS"] = df_schema_metadata.loc[df_redundancy.index, "NUM_NULLS"].values if "NUM_NULLS" in df_schema_metadata.columns else ""
            df_redundancy["NUM_DISTINCT"] = df_schema_metadata.loc[df_redundancy.index, "NUM_DISTINCT"].values if "NUM_DISTINCT" in df_schema_metadata.columns else ""
            candidate_frames.append(df_redundancy)

        if not candidate_frames:
            return pd.DataFrame()

        return pd.concat(candidate_frames, ignore_index=True)
