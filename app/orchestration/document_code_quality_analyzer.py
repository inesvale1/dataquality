from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from dataquality.domain.validators.document_code_classifier import classify_document_code
from dataquality.infrastructure.io.csv.document_code_loader import DocumentCodeLoader

_ISSUE_CLASSIFICATIONS = {"INVALIDO", "AMBIGUO"}
_RULE_MAP = {"INVALIDO": "MQID015", "AMBIGUO": "MQID015"}
_DESC_MAP = {
    "INVALIDO": "Invalid document code (CPF/CNPJ/CGF)",
    "AMBIGUO": "Ambiguous document code (CPF/CNPJ/CGF)",
}
_EMPTY_ISSUES_COLUMNS = ["rule", "desc", "owner", "table", "column", "value"]


@dataclass(frozen=True)
class DocumentCodeAnalysisResult:
    total_distinct_codes: int
    invalid_or_ambiguous_distinct_codes: int
    issues_df: pd.DataFrame
    classifications_df: pd.DataFrame


class DocumentCodeQualityAnalyzer:
    def __init__(self, base_folder: Path):
        self.loader = DocumentCodeLoader(base_folder)

    def analyze_schema(self, schema_name: str) -> DocumentCodeAnalysisResult:
        input_files = self.loader.list_schema_files(schema_name)
        total_count = 0
        issue_rows: list[dict[str, object]] = []

        if input_files:
            max_workers = min(8, len(input_files))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for file_total, file_issues in executor.map(self._analyze_input_file, input_files):
                    total_count += file_total
                    issue_rows.extend(file_issues)

        if not issue_rows:
            return DocumentCodeAnalysisResult(
                total_distinct_codes=total_count,
                invalid_or_ambiguous_distinct_codes=0,
                issues_df=pd.DataFrame(columns=_EMPTY_ISSUES_COLUMNS),
                classifications_df=pd.DataFrame(),
            )

        issues_df = pd.DataFrame(issue_rows)
        issues_df["rule"] = issues_df["classification"].map(_RULE_MAP)
        issues_df["desc"] = issues_df["classification"].map(_DESC_MAP)
        issues_df = issues_df[_EMPTY_ISSUES_COLUMNS].reset_index(drop=True)

        return DocumentCodeAnalysisResult(
            total_distinct_codes=total_count,
            invalid_or_ambiguous_distinct_codes=len(issue_rows),
            issues_df=issues_df,
            classifications_df=pd.DataFrame(),
        )

    def _analyze_input_file(self, input_file) -> tuple[int, list[dict[str, object]]]:
        column_name, values = self.loader.load_values(input_file)
        total = 0
        issue_rows: list[dict[str, object]] = []
        for value in values:
            classification = classify_document_code(column_name, value)
            if not classification.normalized_value:
                continue
            total += 1
            if classification.classification in _ISSUE_CLASSIFICATIONS:
                issue_rows.append(
                    {
                        "owner": input_file.owner,
                        "table": input_file.table,
                        "column": column_name,
                        "value": str(value).strip(),
                        "classification": classification.classification,
                    }
                )
        return total, issue_rows
