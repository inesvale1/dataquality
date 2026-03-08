from __future__ import annotations

import re
from typing import Dict

import pandas as pd

from dataquality.shared.utils import safe_iqmd


class DataQualityValidator:
    def __init__(self, invalid_example_limit: int = 5):
        self.invalid_example_limit = invalid_example_limit

    def validate_format_conformity(
        self,
        schema_name: str,
        candidates_df: pd.DataFrame,
        samples_by_table: Dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        candidate_rows = []
        metric_rows = []
        issue_rows = []

        if candidates_df is None or candidates_df.empty:
            return {
                "DATA_QUALITY_RULE_CANDIDATES": pd.DataFrame(),
                "DATA_QUALITY_METRICS": pd.DataFrame(),
                "DATA_QUALITY_ISSUES": pd.DataFrame(),
            }

        for _, candidate in candidates_df.iterrows():
            table_name = str(candidate.get("TABLE_NAME", "")).upper()
            column_name = str(candidate.get("COLUMN_NAME", "")).upper()
            sample_df = samples_by_table.get(table_name)

            candidate_rows.append(candidate.to_dict())

            if sample_df is None or sample_df.empty:
                issue_rows.append(
                    self._build_issue_row(
                        schema_name=schema_name,
                        candidate=candidate,
                        status="MISSING_SAMPLE",
                        message="Sample file for table was not found.",
                    )
                )
                metric_rows.append(self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE"))
                continue

            if column_name not in sample_df.columns:
                issue_rows.append(
                    self._build_issue_row(
                        schema_name=schema_name,
                        candidate=candidate,
                        status="MISSING_COLUMN",
                        message="Candidate column was not found in the sample file.",
                    )
                )
                metric_rows.append(self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE"))
                continue

            series = sample_df[column_name]
            non_null_mask = series.notna() & series.astype(str).str.strip().ne("")
            evaluated_values = series[non_null_mask].astype(str).str.strip()
            evaluated_rows = int(evaluated_values.shape[0])

            if evaluated_rows == 0:
                issue_rows.append(
                    self._build_issue_row(
                        schema_name=schema_name,
                        candidate=candidate,
                        status="NO_VALUES",
                        message="No non-null values were available in the sample.",
                    )
                )
                metric_rows.append(self._build_metric_row(schema_name, candidate, 0, 0, 0, "NOT_AVAILABLE"))
                continue

            regex = re.compile(str(candidate.get("FORMAT_CONFORMITY_EXPECTED_FORMAT", "")))
            valid_mask = evaluated_values.apply(lambda value: bool(regex.fullmatch(value)))
            valid_rows = int(valid_mask.sum())
            invalid_rows = int(evaluated_rows - valid_rows)

            if invalid_rows > 0:
                invalid_examples = evaluated_values[~valid_mask].head(self.invalid_example_limit).tolist()
                issue_rows.append(
                    self._build_issue_row(
                        schema_name=schema_name,
                        candidate=candidate,
                        status="INVALID_FORMAT",
                        message="Sample contains values outside the expected format.",
                        invalid_count=invalid_rows,
                        invalid_examples=invalid_examples,
                    )
                )

            metric_rows.append(self._build_metric_row(schema_name, candidate, evaluated_rows, valid_rows, invalid_rows, "CALCULATED"))

        return {
            "DATA_QUALITY_RULE_CANDIDATES": pd.DataFrame(candidate_rows),
            "DATA_QUALITY_METRICS": pd.DataFrame(metric_rows),
            "DATA_QUALITY_ISSUES": pd.DataFrame(issue_rows),
        }

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
            "Metric": candidate.get("FORMAT_CONFORMITY_METRIC", ""),
            "Dimension": candidate.get("FORMAT_CONFORMITY_DIMENSION", ""),
            "SemanticTag": candidate.get("FORMAT_CONFORMITY_SEMANTIC_TAG", ""),
            "Priority": candidate.get("FORMAT_CONFORMITY_PRIORITY", ""),
            "RuleType": candidate.get("FORMAT_CONFORMITY_RULE_TYPE", ""),
            "ExpectedFormat": candidate.get("FORMAT_CONFORMITY_EXPECTED_FORMAT", ""),
            "EvaluatedRows": evaluated_rows,
            "ValidRows": valid_rows,
            "InvalidRows": invalid_rows,
            "Value": f"{value:.2f}%" if status == "CALCULATED" else "N/A",
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
            "Metric": candidate.get("FORMAT_CONFORMITY_METRIC", ""),
            "Dimension": candidate.get("FORMAT_CONFORMITY_DIMENSION", ""),
            "SemanticTag": candidate.get("FORMAT_CONFORMITY_SEMANTIC_TAG", ""),
            "Priority": candidate.get("FORMAT_CONFORMITY_PRIORITY", ""),
            "Status": status,
            "InvalidCount": invalid_count,
            "Message": message,
            "InvalidExamples": examples,
        }
