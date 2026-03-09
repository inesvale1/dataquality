from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SemanticFormatRuleSpec:
    semantic_key: str
    metric: str
    dimension: str
    rule_type: str
    expected_format: str
    priority: str
    description: str
    name_patterns: tuple[str, ...]


SEMANTIC_FORMAT_RULES: tuple[SemanticFormatRuleSpec, ...] = (
    SemanticFormatRuleSpec(
        semantic_key="EMAIL",
        metric="Format Conformity",
        dimension="Consistency",
        rule_type="regex",
        expected_format=r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
        priority="high",
        description="Coluna textual com padrao esperado de e-mail",
        name_patterns=(r"(^|_)(EMAIL|E_MAIL)(_|$)",),
    ),
    SemanticFormatRuleSpec(
        semantic_key="CPF",
        metric="Format Conformity",
        dimension="Consistency",
        rule_type="document_validator",
        expected_format=r"^\d{11}$|^\d{3}\.\d{3}\.\d{3}-\d{2}$",
        priority="high",
        description="Coluna textual com padrao esperado de CPF",
        name_patterns=(r"(^|_)CPF(_|$)",),
    ),
    SemanticFormatRuleSpec(
        semantic_key="CNPJ",
        metric="Format Conformity",
        dimension="Consistency",
        rule_type="document_validator",
        expected_format=r"^[A-Za-z0-9]{12}\d{2}$|^\d{14}$",
        priority="high",
        description="Coluna textual com padrao esperado de CNPJ numerico ou alfanumerico",
        name_patterns=(r"(^|_)CNPJ(_|$)",),
    ),
    SemanticFormatRuleSpec(
        semantic_key="CEP",
        metric="Format Conformity",
        dimension="Consistency",
        rule_type="numeric_string",
        expected_format=r"^\d{8}$|^\d{5}-\d{3}$",
        priority="high",
        description="Coluna textual com padrao esperado de CEP",
        name_patterns=(r"(^|_)CEP(_|$)",),
    ),
    SemanticFormatRuleSpec(
        semantic_key="TELEFONE",
        metric="Format Conformity",
        dimension="Consistency",
        rule_type="numeric_string",
        expected_format=r"^\d{10,11}$|^\(\d{2}\)\d{4,5}-\d{4}$",
        priority="high",
        description="Coluna textual com padrao esperado de telefone",
        name_patterns=(r"(^|_)(TEL|TELEFONE|FONE)(_|$)",),
    ),
    SemanticFormatRuleSpec(
        semantic_key="CELULAR",
        metric="Format Conformity",
        dimension="Consistency",
        rule_type="numeric_string",
        expected_format=r"^\d{10,11}$|^\(\d{2}\)\d{4,5}-\d{4}$",
        priority="high",
        description="Coluna textual com padrao esperado de celular",
        name_patterns=(r"(^|_)(CEL|CELULAR)(_|$)",),
    ),
    SemanticFormatRuleSpec(
        semantic_key="DAT",
        metric="Format Conformity",
        dimension="Consistency",
        rule_type="date_string",
        expected_format=r"^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$",
        priority="medium",
        description="Coluna textual com padrao esperado de data",
        name_patterns=(r"(^|_)(DAT|DATA|DT)(_|$)",),
    ),
)
