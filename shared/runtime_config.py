from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from dataquality.domain.config.validation_config import NamedLengthRule, TypeNamingRuleConfig, ValidationConfig


def load_json_config(path: str | None) -> dict[str, Any]:
    if not path:
        return {}

    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError("JSON config must be an object at the top level.")
    return data


def get_config_value(config: dict[str, Any], key: str, default: Any = None) -> Any:
    normalized_target = _normalize_key(key)
    for raw_key, value in config.items():
        if _normalize_key(raw_key) == normalized_target:
            return value
    return default


def build_validation_config(raw: dict[str, Any] | None) -> ValidationConfig | None:
    if not raw:
        return None

    config_defaults = asdict(ValidationConfig())
    type_defaults = asdict(TypeNamingRuleConfig())

    validation_kwargs: dict[str, Any] = {}
    for key, default in config_defaults.items():
        if key == "type_naming":
            continue
        validation_kwargs[key] = get_config_value(raw, key, default)

    raw_type_naming = get_config_value(raw, "type_naming", {}) or {}
    if not isinstance(raw_type_naming, dict):
        raise ValueError("validation_config.type_naming must be a JSON object.")

    type_kwargs: dict[str, Any] = {}
    for key, default in type_defaults.items():
        if key == "named_length_rules":
            continue
        type_kwargs[key] = get_config_value(raw_type_naming, key, default)

    raw_named_rules = get_config_value(raw_type_naming, "named_length_rules", type_defaults["named_length_rules"])
    named_rules: list[NamedLengthRule] = []
    for item in raw_named_rules:
        if not isinstance(item, dict):
            raise ValueError("Each named_length_rules entry must be a JSON object.")
        named_rules.append(
            NamedLengthRule(
                token=str(get_config_value(item, "token", "")),
                allowed_lengths=list(get_config_value(item, "allowed_lengths", [])),
                max_length=get_config_value(item, "max_length", None),
                expected_description=str(get_config_value(item, "expected_description", "")),
            )
        )

    type_kwargs["named_length_rules"] = named_rules
    validation_kwargs["type_naming"] = TypeNamingRuleConfig(**type_kwargs)
    return ValidationConfig(**validation_kwargs)


def build_data_quality_config_template() -> dict[str, Any]:
    return {
        "metadata_base_folder": "dataquality\\schema",
        "sample_base_folder": "dataquality\\samples",
        "sample_source": "csv",
        "db_connection_uri": None,
        "db_authentication_type": "username_password",
        "db_driver_class_name": None,
        "sample_query_template": None,
        "sample_limit": 1000,
        "telemetry_output": None,
        "delete_cols": ["COLUMN_ID", "NUM_BUCKETS", "DENSITY"],
        "plural_exceptions": ["DAS", "INS", "SUBS", "ICMS"],
        "db_type": "Oracle",
        "exclude_tables": ["RUPD$", "VW", "SUANOTA.NFP_DADOS_CADASTRAIS_HIST_BKP2", "MLOG$_"],
        "validation_config": _build_validation_config_template(),
    }


def build_model_quality_config_template() -> dict[str, Any]:
    return {
        "base_folder": "dataquality\\schema",
        "telemetry_output": None,
        "delete_cols": ["COLUMN_ID", "NUM_BUCKETS", "DENSITY"],
        "plural_exceptions": ["DAS", "INS", "SUBS", "ICMS"],
        "db_type": "Oracle",
        "exclude_tables": ["RUPD$", "VW", "SUANOTA.NFP_DADOS_CADASTRAIS_HIST_BKP2", "MLOG$_"],
        "validation_config": _build_validation_config_template(),
    }


def _build_validation_config_template() -> dict[str, Any]:
    return {
        "max_table_len": 30,
        "max_column_len": 30,
        "prefix_names": [
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
        ],
        "pk_prefix": "PK_",
        "fk_prefix": "FK_",
        "unique_suffix_regex": r".*_UK(\d+)?$",
        "type_naming": {
            "date_prefixes": ["DAT_"],
            "indicator_prefixes": ["FLG_", "IND_"],
            "value_prefixes": ["VLR_"],
            "quantity_prefixes": ["QTD_"],
            "code_prefixes": ["COD_"],
            "identifier_name_patterns": [
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
            ],
            "indicator_text_lengths": [1],
            "indicator_numeric_lengths": [1],
            "indicator_numeric_scales": [0],
            "quantity_numeric_scales": [0],
            "code_max_text_length": 64,
            "named_length_rules": [
                {"token": "CPF", "allowed_lengths": [11, 14], "max_length": None, "expected_description": "CHAR/VARCHAR2 length 11 or 14"},
                {"token": "CNPJ", "allowed_lengths": [14, 18], "max_length": None, "expected_description": "CHAR/VARCHAR2 length 14 or 18"},
                {"token": "CEP", "allowed_lengths": [8, 9], "max_length": None, "expected_description": "CHAR/VARCHAR2 length 8 or 9"},
                {"token": "UF", "allowed_lengths": [2], "max_length": None, "expected_description": "CHAR/VARCHAR2 length 2"},
                {"token": "EMAIL", "allowed_lengths": [], "max_length": 254, "expected_description": "text up to 254 chars"},
                {"token": "PLACA", "allowed_lengths": [7, 8], "max_length": None, "expected_description": "CHAR/VARCHAR2 length 7 or 8"},
            ],
        },
    }


def _normalize_key(key: str) -> str:
    return str(key).strip().lower().replace("-", "_")
