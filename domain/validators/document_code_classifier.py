from __future__ import annotations

from dataclasses import dataclass
import re

from dataquality.domain.validators.br_documents import (
    clean_alphanumeric_document,
    clean_numeric_document,
    is_valid_cnpj,
    is_valid_cpf,
    normalize_numeric_cnpj_for_validation,
    normalize_numeric_cpf_for_validation,
)


@dataclass(frozen=True)
class DocumentCodeClassification:
    classification: str
    confidence: str
    normalized_value: str
    numeric_value: str
    cpf_valid: bool
    cnpj_valid: bool


def classify_document_code(column_name: str, value: object) -> DocumentCodeClassification:
    strategy = _infer_validation_strategy(column_name)
    raw_text = "" if value is None else str(value).strip()
    numeric_value = clean_numeric_document(raw_text)
    normalized_value = clean_alphanumeric_document(raw_text)

    cpf_valid = False
    cnpj_valid = False

    if strategy in {"CPF_ONLY", "MIXED"}:
        cpf_valid = _is_valid_cpf_candidate(raw_text)
    if strategy in {"CNPJ_ONLY", "MIXED"}:
        cnpj_valid = _is_valid_cnpj_candidate(raw_text)

    if strategy == "CPF_ONLY":
        classification = "CPF" if cpf_valid else "INVALIDO"
    elif strategy == "CNPJ_ONLY":
        classification = "CNPJ" if cnpj_valid else "INVALIDO"
    elif strategy == "MIXED":
        if cpf_valid and cnpj_valid:
            classification = "AMBIGUO"
        elif cpf_valid:
            classification = "CPF"
        elif cnpj_valid:
            classification = "CNPJ"
        else:
            classification = "INVALIDO"
    else:
        classification = "INVALIDO"

    if classification == "CNPJ" and numeric_value and normalized_value.isdigit():
        canonical_cnpj = normalize_numeric_cnpj_for_validation(numeric_value)
        if len(canonical_cnpj) == 14:
            normalized_value = canonical_cnpj
            numeric_value = canonical_cnpj

    if classification == "CPF" and numeric_value and normalized_value.isdigit():
        canonical_cpf = normalize_numeric_cpf_for_validation(numeric_value)
        if len(canonical_cpf) == 11:
            normalized_value = canonical_cpf
            numeric_value = canonical_cpf

    confidence = _infer_confidence(classification, numeric_value, normalized_value)
    return DocumentCodeClassification(
        classification=classification,
        confidence=confidence,
        normalized_value=normalized_value,
        numeric_value=numeric_value,
        cpf_valid=cpf_valid,
        cnpj_valid=cnpj_valid,
    )


def _infer_validation_strategy(column_name: str) -> str:
    normalized_name = re.sub(r"[^A-Z0-9]+", "_", str(column_name or "").upper()).strip("_")
    has_cpf = bool(re.search(r"(^|_)CPF(_|$)", normalized_name))
    has_cnpj = bool(re.search(r"(^|_)CNPJ(_|$)", normalized_name))
    has_cod_contr = bool(re.search(r"(^|_)COD_CONTR(_|$)", normalized_name))

    if has_cod_contr or (has_cpf and has_cnpj):
        return "MIXED"
    if has_cpf:
        return "CPF_ONLY"
    if has_cnpj:
        return "CNPJ_ONLY"
    return "UNSUPPORTED"


def _is_valid_cpf_candidate(value: object) -> bool:
    digits = clean_numeric_document(value)
    if not digits or len(digits) > 11:
        return False
    return is_valid_cpf(digits)


def _is_valid_cnpj_candidate(value: object) -> bool:
    raw_text = "" if value is None else str(value).strip()
    normalized_value = clean_alphanumeric_document(raw_text)
    numeric_value = clean_numeric_document(raw_text)

    if not normalized_value:
        return False

    if numeric_value and normalized_value.isdigit():
        if len(numeric_value) > 14:
            return False
        return is_valid_cnpj(numeric_value)

    if len(normalized_value) == 14:
        return is_valid_cnpj(normalized_value)
    return False


def _infer_confidence(classification: str, numeric_value: str, normalized_value: str) -> str:
    if classification == "INVALIDO":
        return ""
    if classification == "AMBIGUO":
        return "BAIXA"

    if classification == "CPF":
        length = len(numeric_value)
        if length == 11:
            return "ALTA"
        if length in {9, 10}:
            return "MEDIA"
        return "BAIXA"

    if classification == "CNPJ":
        length = len(numeric_value) if numeric_value else len(normalized_value)
        if length == 14:
            return "ALTA"
        if length in {12, 13}:
            return "MEDIA"
        return "BAIXA"

    return ""
