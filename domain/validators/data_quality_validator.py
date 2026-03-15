from __future__ import annotations

import re
from typing import Dict

import pandas as pd

from dataquality.domain.validators.br_documents import is_valid_cnpj, is_valid_cpf
from dataquality.shared.telemetry import get_current_telemetry
from dataquality.shared.utils import safe_iqmd


class DataQualityValidator:
    _NULL_TEXT_VALUES = {"", "nan", "null", "none"}

    def __init__(self, invalid_example_limit: int = 5):
        self.invalid_example_limit = invalid_example_limit

    def validate_candidates(
        self,
        schema_name: str,
        candidates_df: pd.DataFrame,
        samples_by_table: Dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        telemetry = get_current_telemetry()
        candidate_rows = []
        metric_rows = []
        issue_rows = []

        if candidates_df is None or candidates_df.empty:
            return {
                "DATA_QUALITY_RULE_CANDIDATES": pd.DataFrame(),
                "DATA_QUALITY_METRICS": pd.DataFrame(),
                "DATA_QUALITY_ISSUES": pd.DataFrame(),
            }

        if telemetry is not None:
            telemetry.increment("candidates_processed", int(candidates_df.shape[0]), schema=schema_name)
            with telemetry.stage("data_quality_validator.validate_candidates", schema=schema_name):
                for _, candidate in candidates_df.iterrows():
                    candidate_rows.append(candidate.to_dict())
                    metric_name = str(candidate.get("METRIC", "")).strip()

                    if metric_name == "Format Conformity":
                        metric_row, issue_row = self._validate_format_conformity_candidate(schema_name, candidate, samples_by_table)
                        metric_rows.append(metric_row)
                        if issue_row:
                            issue_rows.append(issue_row)
                        continue

                    if metric_name == "Redundancy detection":
                        redundancy_metrics, issue_row = self._validate_redundancy_candidate(schema_name, candidate, samples_by_table)
                        metric_rows.extend(redundancy_metrics)
                        if issue_row:
                            issue_rows.append(issue_row)
                        continue

                    issue_rows.append(
                        self._build_issue_row(
                            schema_name=schema_name,
                            candidate=candidate,
                            status="UNSUPPORTED_METRIC",
                            message=f"Metric '{metric_name}' is not supported by the validator.",
                        )
                    )
        else:
            for _, candidate in candidates_df.iterrows():
                candidate_rows.append(candidate.to_dict())
                metric_name = str(candidate.get("METRIC", "")).strip()

                if metric_name == "Format Conformity":
                    metric_row, issue_row = self._validate_format_conformity_candidate(schema_name, candidate, samples_by_table)
                    metric_rows.append(metric_row)
                    if issue_row:
                        issue_rows.append(issue_row)
                    continue

                if metric_name == "Redundancy detection":
                    redundancy_metrics, issue_row = self._validate_redundancy_candidate(schema_name, candidate, samples_by_table)
                    metric_rows.extend(redundancy_metrics)
                    if issue_row:
                        issue_rows.append(issue_row)
                    continue

                issue_rows.append(
                    self._build_issue_row(
                        schema_name=schema_name,
                        candidate=candidate,
                        status="UNSUPPORTED_METRIC",
                        message=f"Metric '{metric_name}' is not supported by the validator.",
                    )
                )

        if telemetry is not None:
            telemetry.set_gauge("data_quality_metric_rows", int(len(metric_rows)), schema=schema_name)
            telemetry.set_gauge("data_quality_issue_rows", int(len(issue_rows)), schema=schema_name)

        return {
            "DATA_QUALITY_RULE_CANDIDATES": pd.DataFrame(candidate_rows),
            "DATA_QUALITY_METRICS": pd.DataFrame(metric_rows),
            "DATA_QUALITY_ISSUES": pd.DataFrame(issue_rows),
        }

    def validate_format_conformity(
        self,
        schema_name: str,
        candidates_df: pd.DataFrame,
        samples_by_table: Dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        return self.validate_candidates(schema_name, candidates_df, samples_by_table)

    def _validate_format_conformity_candidate(
        self,
        schema_name: str,
        candidate: pd.Series,
        samples_by_table: Dict[str, pd.DataFrame],
    ) -> tuple[dict[str, object], dict[str, object] | None]:
        table_name = str(candidate.get("TABLE_NAME", "")).upper()
        column_name = str(candidate.get("COLUMN_NAME", "")).upper()
        sample_df = samples_by_table.get(table_name)

        if sample_df is None or sample_df.empty:
            issue = self._build_issue_row(
                schema_name=schema_name,
                candidate=candidate,
                status="MISSING_SAMPLE",
                message="Sample file for table was not found.",
            )
            return self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE"), issue

        if column_name not in sample_df.columns:
            issue = self._build_issue_row(
                schema_name=schema_name,
                candidate=candidate,
                status="MISSING_COLUMN",
                message="Candidate column was not found in the sample file.",
            )
            return self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE"), issue

        series = sample_df[column_name]
        non_null_mask = self._build_non_null_mask(series)
        evaluated_values = series[non_null_mask].astype(str).str.strip()
        evaluated_rows = int(evaluated_values.shape[0])

        if evaluated_rows == 0:
            issue = self._build_issue_row(
                schema_name=schema_name,
                candidate=candidate,
                status="NO_VALUES",
                message="No non-null values were available in the sample.",
            )
            return self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE"), issue

        valid_mask = self._build_format_conformity_mask(evaluated_values, candidate)
        valid_rows = int(valid_mask.sum())
        invalid_rows = int(evaluated_rows - valid_rows)

        issue = None
        if invalid_rows > 0:
            invalid_examples = evaluated_values[~valid_mask].head(self.invalid_example_limit).tolist()
            issue = self._build_issue_row(
                schema_name=schema_name,
                candidate=candidate,
                status="INVALID_FORMAT",
                message="Sample contains values outside the expected format.",
                invalid_count=invalid_rows,
                invalid_examples=invalid_examples,
            )

        return self._build_metric_row(schema_name, candidate, evaluated_rows, valid_rows, invalid_rows, "CALCULATED"), issue

    def _build_format_conformity_mask(self, values: pd.Series, candidate: pd.Series) -> pd.Series:
        semantic_tag = str(candidate.get("SEMANTIC_TAG", "")).strip().upper()
        if semantic_tag == "CPF":
            return values.apply(is_valid_cpf)
        if semantic_tag == "CNPJ":
            return values.apply(is_valid_cnpj)

        regex = re.compile(str(candidate.get("EXPECTED_FORMAT", "")))
        return values.apply(lambda value: bool(regex.fullmatch(value)))

    def _validate_redundancy_candidate(
        self,
        schema_name: str,
        candidate: pd.Series,
        samples_by_table: Dict[str, pd.DataFrame],
    ) -> tuple[list[dict[str, object]], dict[str, object] | None]:
        method = str(candidate.get("CALCULATION_METHOD", "")).strip().upper()
        if method == "METADATA_STATISTICS":
            return self._build_stats_based_uniqueness_metrics(schema_name, candidate), None

        table_name = str(candidate.get("TABLE_NAME", "")).upper()
        column_name = str(candidate.get("COLUMN_NAME", "")).upper()
        sample_df = samples_by_table.get(table_name)

        if sample_df is None or sample_df.empty:
            issue = self._build_issue_row(
                schema_name=schema_name,
                candidate=candidate,
                status="MISSING_SAMPLE",
                message="Sample file for table was not found for redundancy detection.",
            )
            return [self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE")], issue

        if column_name not in sample_df.columns:
            issue = self._build_issue_row(
                schema_name=schema_name,
                candidate=candidate,
                status="MISSING_COLUMN",
                message="Candidate column was not found in the sample file.",
            )
            return [self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE")], issue

        series = sample_df[column_name]
        evaluated_values = series[self._build_non_null_mask(series)].astype(str).str.strip()
        evaluated_rows = int(evaluated_values.shape[0])

        if evaluated_rows == 0:
            issue = self._build_issue_row(
                schema_name=schema_name,
                candidate=candidate,
                status="NO_VALUES",
                message="No non-null values were available in the sample.",
            )
            return [self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE")], issue

        valid_rows = int(evaluated_values.nunique(dropna=True))
        invalid_rows = max(evaluated_rows - valid_rows, 0)
        metrics = [
            self._build_metric_row(schema_name, candidate, evaluated_rows, valid_rows, invalid_rows, "CALCULATED"),
            self._build_companion_uniqueness_row(schema_name, candidate, evaluated_rows, valid_rows, "CALCULATED_SAMPLE"),
        ]

        issue = None
        if invalid_rows > 0:
            duplicate_examples = (
                evaluated_values.value_counts()
                .loc[lambda s: s > 1]
                .head(self.invalid_example_limit)
                .index.tolist()
            )
            issue = self._build_issue_row(
                schema_name=schema_name,
                candidate=candidate,
                status="DUPLICATE_VALUES",
                message="Sample contains repeated values for the candidate column.",
                invalid_count=invalid_rows,
                invalid_examples=[str(value) for value in duplicate_examples],
            )
        return metrics, issue

    def _build_stats_based_uniqueness_metrics(
        self,
        schema_name: str,
        candidate: pd.Series,
    ) -> list[dict[str, object]]:
        num_rows = self._to_int(candidate.get("NUM_ROWS", 0))
        num_nulls = self._to_int(candidate.get("NUM_NULLS", 0))
        num_distinct = self._to_int(candidate.get("NUM_DISTINCT", 0))
        evaluated_rows = max(num_rows - num_nulls, 0)
        unique_rows = min(num_distinct, evaluated_rows)
        duplicate_rows = max(evaluated_rows - unique_rows, 0)

        redundancy_row = self._build_metric_row(
            schema_name=schema_name,
            candidate=candidate,
            evaluated_rows=evaluated_rows,
            valid_rows=unique_rows,
            invalid_rows=duplicate_rows,
            status="CALCULATED_METADATA",
        )
        uniqueness_row = self._build_companion_uniqueness_row(
            schema_name=schema_name,
            candidate=candidate,
            evaluated_rows=evaluated_rows,
            unique_rows=unique_rows,
            status="CALCULATED_METADATA",
        )
        return [redundancy_row, uniqueness_row]

    def _build_metric_row(
        self,
        schema_name: str,
        candidate: pd.Series,
        evaluated_rows: int,
        valid_rows: int,
        invalid_rows: int,
        status: str,
    ) -> dict[str, object]:
        value = safe_iqmd(invalid_rows, evaluated_rows)
        return {
            "Schema": schema_name,
            "Owner": candidate.get("OWNER", ""),
            "Table": candidate.get("TABLE_NAME", ""),
            "Column": candidate.get("COLUMN_NAME", ""),
            "Metric": candidate.get("METRIC", ""),
            "Dimension": candidate.get("DIMENSION", ""),
            "SemanticTag": candidate.get("SEMANTIC_TAG", ""),
            "Priority": candidate.get("PRIORITY", ""),
            "RuleType": candidate.get("RULE_TYPE", ""),
            "ExpectedFormat": candidate.get("EXPECTED_FORMAT", ""),
            "CalculationMethod": candidate.get("CALCULATION_METHOD", ""),
            "EvaluatedRows": evaluated_rows,
            "ValidRows": valid_rows,
            "InvalidRows": invalid_rows,
            "Value": f"{value:.2f}" if status.startswith("CALCULATED") else "N/A",
            "Status": status,
        }

    def _build_companion_uniqueness_row(
        self,
        schema_name: str,
        candidate: pd.Series,
        evaluated_rows: int,
        unique_rows: int,
        status: str,
    ) -> dict[str, object]:
        uniqueness = 0.0 if not evaluated_rows else float((unique_rows / evaluated_rows) * 100)
        return {
            "Schema": schema_name,
            "Owner": candidate.get("OWNER", ""),
            "Table": candidate.get("TABLE_NAME", ""),
            "Column": candidate.get("COLUMN_NAME", ""),
            "Metric": "Uniqueness",
            "Dimension": "Uniqueness",
            "SemanticTag": candidate.get("SEMANTIC_TAG", ""),
            "Priority": candidate.get("PRIORITY", ""),
            "RuleType": "distinct_ratio",
            "ExpectedFormat": "",
            "CalculationMethod": candidate.get("CALCULATION_METHOD", ""),
            "EvaluatedRows": evaluated_rows,
            "ValidRows": unique_rows,
            "InvalidRows": max(evaluated_rows - unique_rows, 0),
            "Value": f"{uniqueness:.2f}" if status.startswith("CALCULATED") else "N/A",
            "Status": status,
        }

    def _build_issue_row(
        self,
        schema_name: str,
        candidate: pd.Series,
        status: str,
        message: str,
        invalid_count: int = 0,
        invalid_examples: list[str] | None = None,
    ) -> dict[str, object]:
        examples = " | ".join(invalid_examples or [])
        return {
            "Schema": schema_name,
            "Owner": candidate.get("OWNER", ""),
            "Table": candidate.get("TABLE_NAME", ""),
            "Column": candidate.get("COLUMN_NAME", ""),
            "Metric": candidate.get("METRIC", ""),
            "Dimension": candidate.get("DIMENSION", ""),
            "SemanticTag": candidate.get("SEMANTIC_TAG", ""),
            "Priority": candidate.get("PRIORITY", ""),
            "CalculationMethod": candidate.get("CALCULATION_METHOD", ""),
            "Status": status,
            "InvalidCount": invalid_count,
            "Message": message,
            "InvalidExamples": examples,
        }

    def _to_int(self, value: object) -> int:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric):
            return 0
        return int(numeric)

    def _build_non_null_mask(self, series: pd.Series) -> pd.Series:
        normalized = series.astype(str).str.strip().str.lower()
        return series.notna() & ~normalized.isin(self._NULL_TEXT_VALUES)
