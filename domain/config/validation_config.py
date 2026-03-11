from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class NamedLengthRule:
    token: str
    allowed_lengths: List[int] = field(default_factory=list)
    max_length: int | None = None
    expected_description: str = ""


@dataclass(frozen=True)
class TypeNamingRuleConfig:
    date_prefixes: List[str] = field(default_factory=lambda: ["DAT_"])
    indicator_prefixes: List[str] = field(default_factory=lambda: ["FLG_", "IND_"])
    value_prefixes: List[str] = field(default_factory=lambda: ["VLR_"])
    quantity_prefixes: List[str] = field(default_factory=lambda: ["QTD_"])
    code_prefixes: List[str] = field(default_factory=lambda: ["COD_"])
    identifier_name_patterns: List[str] = field(
        default_factory=lambda: [
            r"(^|_)ID($|_)",
            r"(^|_)COD($|_)",
            r"(^|_)CPF($|_)",
            r"(^|_)CNPJ($|_)",
            r"(^|_)CEP($|_)",
            r"(^|_)EMAIL($|_)",
            r"(^|_)PLACA($|_)",
            r"(^|_)RENAVAM($|_)",
            r"(^|_)CHASSI($|_)",
            r"(^|_)MATRICULA($|_)",
            r"(^|_)PROTOCOLO($|_)",
        ]
    )
    indicator_text_lengths: List[int] = field(default_factory=lambda: [1])
    indicator_numeric_lengths: List[int] = field(default_factory=lambda: [1])
    indicator_numeric_scales: List[int] = field(default_factory=lambda: [0])
    quantity_numeric_scales: List[int] = field(default_factory=lambda: [0])
    code_max_text_length: int = 64
    named_length_rules: List[NamedLengthRule] = field(
        default_factory=lambda: [
            NamedLengthRule("CPF", allowed_lengths=[11, 14], expected_description="CHAR/VARCHAR2 length 11 or 14"),
            NamedLengthRule("CNPJ", allowed_lengths=[14, 18], expected_description="CHAR/VARCHAR2 length 14 or 18"),
            NamedLengthRule("CEP", allowed_lengths=[8, 9], expected_description="CHAR/VARCHAR2 length 8 or 9"),
            NamedLengthRule("UF", allowed_lengths=[2], expected_description="CHAR/VARCHAR2 length 2"),
            NamedLengthRule("EMAIL", max_length=254, expected_description="text up to 254 chars"),
            NamedLengthRule("PLACA", allowed_lengths=[7, 8], expected_description="CHAR/VARCHAR2 length 7 or 8"),
        ]
    )


@dataclass
class ValidationConfig:
    """Configuration for naming/length rules."""

    max_table_len: int = 30
    max_column_len: int = 30

    # Allowed column name prefixes (3 letters + underscore)
    prefix_names: List[str] = field(
        default_factory=lambda: [
            "COD_",
            "DAT_",
            "DSC_",
            "NOM_",
            "NUM_",
            "QTD_",
            "SEQ_",
            "SIT_",
            "STA_",
            "TXT_",
            "TIP_",
            "TOT_",
            "VLR_",
            "BIN_",
            "HOR_",
            "XML_",
        ]
    )

    # Constraint naming patterns
    pk_prefix: str = "PK_"
    fk_prefix: str = "FK_"
    unique_suffix_regex: str = r".*_UK(\d+)?$"  # ends with _UK or _UK<digits>

    # Consistency indicators based on naming/type conventions
    type_naming: TypeNamingRuleConfig = field(default_factory=TypeNamingRuleConfig)
