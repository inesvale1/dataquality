from __future__ import annotations

from pathlib import Path

import pandas as pd

from dataquality.infrastructure.io.pipeline_bridge import ensure_metadata_context
from dataquality.domain.config.llm_comment_config import LLMCommentConfig
from dataquality.domain.config.scoring_config import ScoringConfig
from dataquality.shared.utils import safe_iqmd
from dataquality.domain.config.metadata_metric_config import METADATA_INDICATOR_SPECS
from dataquality.domain.validators.metadata_validator import MetadataValidator
from dataquality.adapters.outbound.exporters.excel_report import build_section_df
from dataquality.domain.scoring.quality_scorer import build_mddq_scores_df, compute_mddq
from dataquality.domain.suggesters.metadata_issue_suggester import (
    AnthropicCommentSuggester,
    LLMCommentSuggester,
    MetadataIssueSuggester,
    OpenAICompatibleCommentSuggester,
)


class MetadataQualityMetricsCalculator:
    def __init__(
        self,
        schema_name: str,
        validator: MetadataValidator,
        df_schema_metadata: pd.DataFrame | None = None,
        db_type: str = "Oracle",
        llm_comment_config: LLMCommentConfig | None = None,
        base_folder: Path | None = None,
        require_metadata_context: bool = True,
        workspace_root: Path | None = None,
        scoring_config: ScoringConfig | None = None,
    ):
        self.schema_name = schema_name
        self.validator = validator
        self.df_schema_metadata = df_schema_metadata
        self.db_type = db_type
        self.llm_comment_config = llm_comment_config or LLMCommentConfig()
        self.base_folder = Path(base_folder) if base_folder else None
        self.require_metadata_context = require_metadata_context
        # dataquality/app/orchestration/ -> dataquality/ -> Implementation/
        self.workspace_root = workspace_root or Path(__file__).resolve().parents[2]
        self.scoring_config = scoring_config or ScoringConfig()

    def calculate_sections(self) -> tuple[dict[str, pd.DataFrame], float | None]:
        """
        Returns (sections, mddq) where sections is a dict of DataFrames and mddq
        is the weighted average metadata quality score (0-100) or None.

        Sections:
        - SCHEMA_METADATA: raw input (candidate annotation columns stripped)
        - DATA_QUALITY_RULE_CANDIDATES: DQ rule candidates derived from metadata
        - METADATA_QUALITY_MEASURES: raw/derived measure totals
        - METADATA_QUALITY_ISSUES: validator issues with LLM suggestions
        - METADATA_SCORES: MDDQ weighted-average breakdown (indicators + aggregate)
        """

        df_schema_metadata = (self.df_schema_metadata.copy() if self.df_schema_metadata is not None else pd.DataFrame())
        df_schema_metadata = self.validator.annotate_data_quality_candidates(df_schema_metadata)
        context_dir = self._resolve_context_dir()
        schema_context = ensure_metadata_context(
            schema_name=self.schema_name,
            inputs_dir=context_dir,
            required=self.require_metadata_context,
            workspace_root=self.workspace_root,
        )
        df_data_quality_candidates = self._build_data_quality_candidates(df_schema_metadata)
        df_issues = self.validator.issues_df.copy()
        suggester = MetadataIssueSuggester(
            db_type=self.db_type,
            schema_context=schema_context,
            llm_comment_suggester=self._build_llm_suggester(schema_context),
            comment_generation_strategy=getattr(self.llm_comment_config, "comment_generation_strategy", "rules"),
        )
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

        _candidate_cols = [
            *MetadataValidator.FORMAT_CONFORMITY_COLUMNS,
            *MetadataValidator.REDUNDANCY_COLUMNS,
        ]
        df_schema_metadata_export = df_schema_metadata.drop(
            columns=[c for c in _candidate_cols if c in df_schema_metadata.columns],
        )

        mddq = compute_mddq(df_metrics, self.scoring_config.metadata_metric_weights)
        df_quality_scores = build_mddq_scores_df(
            df_metrics, self.scoring_config.metadata_metric_weights, mddq
        )

        sections = {
            "SCHEMA_METADATA": df_schema_metadata_export,
            "DATA_QUALITY_RULE_CANDIDATES": df_data_quality_candidates,
            "METADATA_QUALITY_MEASURES": df_measures,
            "METADATA_QUALITY_ISSUES": df_issues,
            "METADATA_SCORES": df_quality_scores,
        }
        return sections, mddq

    def _build_llm_suggester(self, schema_context: dict | None = None) -> LLMCommentSuggester:
        # Built whenever llm_comment_config.enabled is true, independent of
        # comment_generation_strategy: the primary SUGGESTED_VALUE column still
        # follows the strategy, but the SUGGESTED_VALUE_LLM comparison column
        # (populated in MetadataIssueSuggester._suggest_row) needs a live
        # suggester even when the primary strategy is "rules".
        if not self.llm_comment_config.enabled:
            return LLMCommentSuggester(enabled=False)

        api_type = str(getattr(self.llm_comment_config, "api_type", "azure")).strip().lower()
        if api_type == "anthropic":
            context_path = self._resolve_business_context_path()
            metadata_fallback = schema_context if not context_path else None
            return AnthropicCommentSuggester.from_config(
                self.llm_comment_config,
                context_path=context_path,
                metadata_fallback=metadata_fallback,
                business_docs_context_path=self._resolve_business_docs_context_path(),
            )
        return OpenAICompatibleCommentSuggester.from_config(self.llm_comment_config)

    def _resolve_context_dir(self) -> Path:
        if self.base_folder is not None:
            return self.base_folder / self.schema_name / "inputs"
        return self.workspace_root / "schema" / self.schema_name / "inputs"

    def _resolve_business_context_path(self) -> Path | None:
        schema = self.schema_name
        candidates = [
            self.base_folder / schema / "inputs" / f"sources_context_{schema}.json" if self.base_folder else None,
        ]
        for path in candidates:
            if path and path.exists():
                return path
        return None

    def _resolve_business_docs_context_path(self) -> Path | None:
        schema = self.schema_name
        if not self.base_folder:
            return None
        path = self.base_folder / schema / "inputs" / f"business_docs_context_{schema}.json"
        return path if path.exists() else None

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

