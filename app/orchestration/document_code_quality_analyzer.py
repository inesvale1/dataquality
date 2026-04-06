from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from dataquality.domain.validators.document_code_classifier import classify_document_code
from dataquality.infrastructure.io.csv.document_code_loader import DocumentCodeLoader


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
        rows: list[dict[str, object]] = []
        if input_files:
            max_workers = min(8, len(input_files))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for file_rows in executor.map(self._analyze_input_file, input_files):
                    rows.extend(file_rows)

        classifications_df = pd.DataFrame(rows)
        if classifications_df.empty:
            return DocumentCodeAnalysisResult(
                total_distinct_codes=0,
                invalid_or_ambiguous_distinct_codes=0,
                issues_df=pd.DataFrame(columns=["rule", "desc", "owner", "table", "column", "value"]),
                classifications_df=classifications_df,
            )

        distinct_df = classifications_df.drop_duplicates(subset=["distinct_key"]).copy()
        issue_mask = distinct_df["classification"].isin(["INVALIDO", "AMBIGUO"])
        issues_df = distinct_df.loc[issue_mask, ["classification", "owner", "table", "column", "value"]].copy()
        issues_df["rule"] = issues_df["classification"].map(
            {
                "INVALIDO": "MQID015",
                "AMBIGUO": "MQID015",
            }
        )
        issues_df["desc"] = issues_df["classification"].map(
            {
                "INVALIDO": "Invalid CPF/CNPJ",
                "AMBIGUO": "Ambiguous CPF/CNPJ",
            }
        )
        issues_df = issues_df[["rule", "desc", "owner", "table", "column", "value"]]

        return DocumentCodeAnalysisResult(
            total_distinct_codes=int(distinct_df.shape[0]),
            invalid_or_ambiguous_distinct_codes=int(issue_mask.sum()),
            issues_df=issues_df.reset_index(drop=True),
            classifications_df=classifications_df.reset_index(drop=True),
        )

    def _analyze_input_file(self, input_file) -> list[dict[str, object]]:
        column_name, values = self.loader.load_values(input_file)
        rows: list[dict[str, object]] = []
        for value in values:
            classification = classify_document_code(column_name, value)
            if not classification.normalized_value:
                continue
            rows.append(
                {
                    "owner": input_file.owner,
                    "table": input_file.table,
                    "column": column_name,
                    "value": str(value).strip(),
                    "normalized_value": classification.normalized_value,
                    "numeric_value": classification.numeric_value,
                    "classification": classification.classification,
                    "confidence": classification.confidence,
                    "cpf_valid": classification.cpf_valid,
                    "cnpj_valid": classification.cnpj_valid,
                    "distinct_key": (
                        input_file.owner,
                        input_file.table,
                        column_name,
                        classification.normalized_value,
                    ),
                }
            )
        return rows
