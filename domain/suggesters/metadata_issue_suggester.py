from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple
from urllib import error, request

import inflect
import pandas as pd

from dataquality.domain.config.llm_comment_config import LLMCommentConfig
from dataquality.domain.config.validation_config import ValidationConfig


@dataclass
class LLMCommentSuggester:
    enabled: bool = False
    last_error: str = ""

    def suggest_column_comment(self, context: Dict[str, Any]) -> Optional[str]:
        return None

    def suggest_table_comment(self, context: Dict[str, Any]) -> Optional[str]:
        return None


@dataclass
class OpenAICompatibleCommentSuggester(LLMCommentSuggester):
    api_key: str = ""
    model: str = ""
    base_url: str = "https://api.openai.com/v1"
    timeout_seconds: int = 60
    temperature: float = 0.2
    max_output_tokens: int = 180
    disabled_reason: str = ""
    last_error: str = ""
    response_cache: dict[str, Optional[str]] = field(default_factory=dict)

    @classmethod
    def from_config(cls, config: LLMCommentConfig) -> "OpenAICompatibleCommentSuggester":
        api_key = os.getenv(config.api_key_env, "")
        enabled = bool(config.enabled and api_key and config.model)
        if not config.enabled:
            disabled_reason = "LLM_DISABLED"
        elif not api_key:
            disabled_reason = "LLM_MISSING_API_KEY"
        elif not config.model:
            disabled_reason = "LLM_MISSING_MODEL"
        else:
            disabled_reason = ""
        return cls(
            enabled=enabled,
            api_key=api_key,
            model=config.model,
            base_url=config.base_url,
            timeout_seconds=int(config.timeout_seconds),
            temperature=float(config.temperature),
            max_output_tokens=int(config.max_output_tokens),
            disabled_reason=disabled_reason,
        )

    def suggest_column_comment(self, context: Dict[str, Any]) -> Optional[str]:
        return self._suggest(
            context=context,
            entity_label="coluna",
            instructions=(
                "Gere um comentario semantico/de negocio em portugues do Brasil para uma coluna de banco de dados. "
                "Use o contexto da tabela, relacoes e perfil dos dados quando disponivel. "
                "Nao descreva regra tecnica, tipo fisico ou nulabilidade; descreva o significado de negocio."
            ),
        )

    def suggest_table_comment(self, context: Dict[str, Any]) -> Optional[str]:
        return self._suggest(
            context=context,
            entity_label="tabela",
            instructions=(
                "Gere um comentario semantico/de negocio em portugues do Brasil para uma tabela de banco de dados. "
                "Use colunas principais, relacoes, tipo semantico inferido e comentarios existentes nas colunas."
            ),
        )

    def _suggest(self, context: Dict[str, Any], entity_label: str, instructions: str) -> Optional[str]:
        if not self.enabled:
            return None
        compact_context = self._compact_context(context, entity_label)
        cache_key = json.dumps(
            {"entity": entity_label, "context": compact_context},
            ensure_ascii=False,
            sort_keys=True,
        )
        if cache_key in self.response_cache:
            return self.response_cache[cache_key]

        system_prompt = (
            "O comentario deve ter uma unica frase, em portugues, com foco de negocio, objetivo e claro."
            "Utilize um estilo profissional de dicionário de dados."
            "Seja específico, porém conciso."
            "Não invente significados comerciais além do contexto fornecido."
            "Se o contexto for insuficiente, gere um comentário técnico adequado."
            "Dê preferência a descrições que expliquem a função, o significado e o uso de referência."
            "Responda somente com JSON valido no formato {\"comment\": \"...\"}. "
            "Exemplos de comentário seguro:"
            "Código que identifica o status associado ao registro."
            "Data associada ao evento do registro."
            "Valor registrado para a transação."
        )
        user_prompt = (
            f"{instructions}\n"
            f"Entidade: {entity_label}\n"
            "Contexto em JSON:\n"
            f"{json.dumps(compact_context, ensure_ascii=False, indent=2)}"
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": self.temperature,
            "max_tokens": self.max_output_tokens,
        }
        raw_response = self._post_json(payload)
        if not raw_response:
            self.response_cache[cache_key] = None
            return None
        comment = self._extract_comment(raw_response)
        self.response_cache[cache_key] = comment
        return comment

    def _post_json(self, payload: Dict[str, Any]) -> str | None:
        try:
            self.last_error = ""
            endpoint = self.base_url.rstrip("/") + "/chat/completions"
            req = request.Request(
                endpoint,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                body = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            self.last_error = self._format_http_error(exc)
            return None
        except Exception as exc:
            self.last_error = str(exc)
            return None

        choices = body.get("choices", [])
        if not choices:
            self.last_error = "Empty choices returned by LLM API."
            return None
        message = choices[0].get("message", {})
        content = message.get("content", "")
        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text_parts.append(str(item.get("text", "")))
            content = "\n".join(part for part in text_parts if part)
        return str(content).strip() or None

    def _extract_comment(self, raw_response: str) -> Optional[str]:
        match = re.search(r"\{.*\}", raw_response, flags=re.DOTALL)
        candidate = match.group(0) if match else raw_response
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            payload = {"comment": raw_response.strip()}
        comment = str(payload.get("comment", "")).strip()
        return comment or None

    def _compact_context(self, context: Dict[str, Any], entity_label: str) -> Dict[str, Any]:
        if entity_label == "coluna":
            profile = context.get("profile", {}) if isinstance(context.get("profile"), dict) else {}
            references = context.get("references", {}) if isinstance(context.get("references"), dict) else {}
            return {
                "owner": self._limit_text(context.get("owner", ""), 60),
                "table_name": self._limit_text(context.get("table_name", ""), 80),
                "table_comment": self._limit_text(context.get("table_comment", ""), 240),
                "column_name": self._limit_text(context.get("column_name", ""), 80),
                "data_type": self._limit_text(context.get("data_type", ""), 40),
                "nullable": context.get("nullable", ""),
                "is_pk": bool(context.get("is_pk", False)),
                "is_fk": bool(context.get("is_fk", False)),
                "is_uk": bool(context.get("is_uk", False)),
                "references": {
                    "table": self._limit_text(references.get("table", ""), 80),
                    "column": self._limit_text(references.get("column", ""), 80),
                } if references else {},
                "column_neighbors": self._limit_list(context.get("column_neighbors", []), 4, 60),
                "profile": {
                    "num_distinct": profile.get("num_distinct"),
                    "null_ratio": profile.get("null_ratio"),
                },
            }

        existing_comments = context.get("existing_column_comments", {})
        if not isinstance(existing_comments, dict):
            existing_comments = {}
        limited_comments: dict[str, str] = {}
        for idx, (key, value) in enumerate(existing_comments.items()):
            if idx >= 8:
                break
            limited_comments[self._limit_text(key, 80)] = self._limit_text(value, 180)

        return {
            "owner": self._limit_text(context.get("owner", ""), 60),
            "table_name": self._limit_text(context.get("table_name", ""), 80),
            "table_comment": self._limit_text(context.get("table_comment", ""), 240),
            "table_type_inference": self._limit_text(context.get("table_type_inference", ""), 40),
            "primary_keys": self._limit_list(context.get("primary_keys", []), 5, 80),
            "foreign_keys": self._limit_list(context.get("foreign_keys", []), 5, 80),
            "main_columns": self._limit_list(context.get("main_columns", []), 6, 80),
            "related_tables": self._limit_list(context.get("related_tables", []), 6, 80),
            "row_count": context.get("row_count"),
            "column_name_keywords": self._limit_list(context.get("column_name_keywords", []), 12, 40),
            "existing_column_comments": limited_comments,
        }

    def _limit_list(self, values: Any, max_items: int, max_text_length: int) -> list[str]:
        if not isinstance(values, list):
            return []
        return [self._limit_text(value, max_text_length) for value in values[:max_items]]

    def _limit_text(self, value: Any, max_length: int) -> str:
        text = str(value or "").strip()
        if len(text) <= max_length:
            return text
        return text[: max_length - 3].rstrip() + "..."

    def _format_http_error(self, exc: error.HTTPError) -> str:
        detail = ""
        try:
            raw = exc.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                err = parsed.get("error", {})
                if isinstance(err, dict):
                    detail = str(err.get("message", "")).strip()
                if not detail:
                    detail = raw.strip()
            else:
                detail = raw.strip()
        except Exception:
            detail = str(exc)
        status = getattr(exc, "code", "")
        reason = getattr(exc, "reason", "")
        summary = f"HTTP {status} {reason}".strip()
        return f"{summary}: {detail}".strip(": ")


class MetadataIssueSuggester:
    def __init__(
        self,
        db_type: str = "Oracle",
        config: Optional[ValidationConfig] = None,
        llm_comment_suggester: Optional[LLMCommentSuggester] = None,
        schema_context: Optional[Dict[str, Any]] = None,
    ):
        self.db_type = (db_type or "").strip()
        self.config = config or ValidationConfig()
        self.llm_comment_suggester = llm_comment_suggester or LLMCommentSuggester(enabled=False)
        self.schema_context = schema_context or {}
        self.table_context_lookup = self._build_table_context_lookup(self.schema_context)
        self.column_context_lookup = self._build_column_context_lookup(self.schema_context)

    def apply(self, issues_df: pd.DataFrame, schema_df: pd.DataFrame) -> pd.DataFrame:
        if issues_df is None or issues_df.empty:
            return self._ensure_columns(issues_df)

        schema_lookup = self._build_schema_lookup(schema_df)
        suggestions = []
        for _, row in issues_df.iterrows():
            suggestion = self._suggest_row(row, schema_lookup)
            suggestions.append(suggestion)

        df_suggestions = pd.DataFrame(suggestions)
        df_out = issues_df.copy()
        for col in df_suggestions.columns:
            df_out[col] = df_suggestions[col]
        return df_out

    def _ensure_columns(self, issues_df: pd.DataFrame) -> pd.DataFrame:
        df_out = issues_df.copy() if issues_df is not None else pd.DataFrame()
        for col in ("COLUMN_TYPE", "SUGGESTED_VALUE", "SUGGESTED_SOURCE", "SUGGESTED_CONFIDENCE", "SUGGESTED_DDL", "SUGGESTED_DETAIL"):
            if col not in df_out.columns:
                df_out[col] = ""
        return df_out

    def _build_schema_lookup(self, schema_df: pd.DataFrame) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
        if schema_df is None or schema_df.empty:
            return {}
        required = {"OWNER", "TABLE_NAME", "COLUMN_NAME"}
        if not required.issubset(schema_df.columns):
            return {}
        lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for _, row in schema_df.iterrows():
            key = (str(row.get("OWNER", "")).upper(), str(row.get("TABLE_NAME", "")).upper(), str(row.get("COLUMN_NAME", "")).upper())
            if key not in lookup:
                lookup[key] = row.to_dict()
        return lookup

    def _build_table_context_lookup(self, schema_context: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
        lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for item in schema_context.get("tables", []):
            key = (self._clean_str(item.get("owner", "")).upper(), self._clean_str(item.get("table_name", "")).upper())
            if key not in lookup:
                lookup[key] = item
        return lookup

    def _build_column_context_lookup(self, schema_context: Dict[str, Any]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
        lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for item in schema_context.get("columns", []):
            key = (
                self._clean_str(item.get("owner", "")).upper(),
                self._clean_str(item.get("table_name", "")).upper(),
                self._clean_str(item.get("column_name", "")).upper(),
            )
            if key not in lookup:
                lookup[key] = item
        return lookup

    def _suggest_row(self, row: pd.Series, schema_lookup: Dict[Tuple[str, str, str], Dict[str, Any]]) -> Dict[str, Any]:
        rule = self._clean_str(row.get("rule", "")).upper()
        owner = self._clean_str(row.get("owner", "")).upper()
        table = self._clean_str(row.get("table", "")).upper()
        column = self._clean_str(row.get("column", "")).upper()
        constraint_name = self._clean_str(row.get("constraint_name", "")).upper()

        schema_row = schema_lookup.get((owner, table, column), {})
        column_type = self._clean_str(row.get("data_type", "")).upper()
        if not column_type:
            column_type = self._clean_str(schema_row.get("DATA_TYPE", "")).upper()

        suggested_value = ""
        source = ""
        confidence = 0.0

        if rule == "MQME014":
            suggested_value, source, confidence = self._suggest_column_prefix(column, table, column_type)
        elif rule == "MQME008":
            suggested_value, source, confidence = self._suggest_column_comment(owner, table, column)
        elif rule == "MQME027":
            suggested_value, source, confidence = self._suggest_table_comment(owner, table)
        elif rule == "MQME012":
            suggested_value, source, confidence = self._suggest_singular_table(table)
        elif rule == "MQME013":
            suggested_value, source, confidence = self._suggest_shorter_name(table, self.config.max_table_len)
        elif rule == "MQME015":
            suggested_value, source, confidence = self._suggest_shorter_name(column, self.config.max_column_len)
        elif rule == "MQME009":
            suggested_value, source, confidence = self._suggest_constraint_name("PK_", table, column, constraint_name)
        elif rule == "MQME010":
            suggested_value, source, confidence = self._suggest_constraint_name("FK_", table, column, constraint_name)
        elif rule == "MQME011":
            suggested_value, source, confidence = self._suggest_unique_name(table, column, constraint_name)
        elif rule in ("MQME020", "MQME021"):
            suggested_value, source, confidence = "0", "RULES", 0.6

        ddl = self._build_ddl(rule, owner, table, column, constraint_name, suggested_value)
        detail = self._resolve_llm_failure_detail(source)

        return {
            "COLUMN_TYPE": column_type,
            "SUGGESTED_VALUE": suggested_value,
            "SUGGESTED_SOURCE": source,
            "SUGGESTED_CONFIDENCE": confidence,
            "SUGGESTED_DDL": ddl,
            "SUGGESTED_DETAIL": detail,
        }

    def _suggest_column_prefix(self, column: str, table: str, column_type: str) -> Tuple[str, str, float]:
        if not column:
            return "", "", 0.0
        if column.upper() == "DATA" and table:
            return f"DAT_{table}", "RULES", 0.9
        base = self._strip_prefix(column)
        prefix = self._choose_prefix(column_type, base)
        if not prefix:
            return "", "", 0.0
        suggested = prefix + base
        return suggested, "RULES", 0.85

    def _suggest_column_comment(self, owner: str, table: str, column: str) -> Tuple[str, str, float]:
        context = self.column_context_lookup.get((owner, table, column), {})
        if not context:
            return "", "LLM_CONTEXT_MISSING", 0.0
        llm_comment = self.llm_comment_suggester.suggest_column_comment(context)
        if not llm_comment:
            source = self._resolve_llm_failure_source()
            return "", source, 0.0
        return llm_comment, "LLM", 0.9
    
    def _singularize_table_name(self, name: str) -> str:
        p = inflect.engine()

        parts = name.split("_")
        last = parts[-1]

        singular = p.singular_noun(last)
        if singular:
            parts[-1] = singular

        return "_".join(parts)

    def _suggest_singular_table(self, table: str) -> Tuple[str, str, float]:
        if not table:
            return "", "", 0.0

        return self._singularize_table_name(table), "RULES", 0.9

    def _suggest_shorter_name(self, name: str, limit: int) -> Tuple[str, str, float]:
        if not name or len(name) <= limit:
            return "", "", 0.0
        shortened = self._abbreviate(name)
        if len(shortened) > limit:
            shortened = shortened[:limit]
        return shortened, "RULES", 0.6

    def _suggest_constraint_name(self, prefix: str, table: str, column: str, current: str) -> Tuple[str, str, float]:
        if not table:
            return "", "", 0.0
        suggested = f"{prefix}{table}" if not column else f"{prefix}{table}_{column}"
        return suggested, "RULES", 0.8

    def _suggest_unique_name(self, table: str, column: str, current: str) -> Tuple[str, str, float]:
        if not table:
            return "", "", 0.0
        suggested = f"{table}_{column}_UK" if column else f"{table}_UK"
        return suggested, "RULES", 0.8

    def _build_ddl(
        self,
        rule: str,
        owner: str,
        table: str,
        column: str,
        constraint_name: str,
        suggested_value: str,
    ) -> str:
        if not suggested_value:
            return ""
        if self.db_type.lower() != "oracle":
            return ""

        qualified_table = self._qualify_table(owner, table)

        if rule in ("MQME014", "MQME015"):
            return f"ALTER TABLE {qualified_table} RENAME COLUMN {column} TO {suggested_value};"
        if rule in ("MQME012", "MQME013"):
            return f"ALTER TABLE {qualified_table} RENAME TO {suggested_value};"
        if rule == "MQME008":
            comment = suggested_value.replace("'", "''")
            return f"COMMENT ON COLUMN {qualified_table}.{column} IS '{comment}';"
        if rule == "MQME027":
            comment = suggested_value.replace("'", "''")
            return f"COMMENT ON TABLE {qualified_table} IS '{comment}';"
        if rule in ("MQME009", "MQME010", "MQME011"):
            if not constraint_name:
                return ""
            return f"ALTER TABLE {qualified_table} RENAME CONSTRAINT {constraint_name} TO {suggested_value};"

        return ""

    def _qualify_table(self, owner: str, table: str) -> str:
        if owner:
            return f"{owner}.{table}"
        return table

    def _clean_str(self, value: Any) -> str:
        s = str(value).strip()
        if s.lower() in {"", "nan", "none", "<na>", "null"}:
            return ""
        return s

    def _strip_prefix(self, name: str) -> str:
        if not name:
            return name
        parts = name.split("_", 1)
        if len(parts) == 2 and len(parts[0]) == 3:
            return parts[1]
        return name

    def _extract_prefix(self, name: str) -> str:
        parts = name.split("_", 1)
        if len(parts) == 2 and len(parts[0]) == 3:
            return parts[0] + "_"
        return ""

    def _choose_prefix(self, data_type: str, base: str) -> str:
        data_type = data_type.upper()
        base_upper = base.upper()

        data_type_map = {
            "NUMBER": "NUM_",
            "DATE": "DAT_",
            "TIMESTAMP": "DAT_",
            "CHAR": "DSC_",
            "NCHAR": "DSC_",
            "VARCHAR2": "DSC_",
            "NVARCHAR2": "DSC_",
            "CLOB": "TXT_",
            "RAW": "BIN_",
            "BLOB": "BIN_",
        }
        if data_type in data_type_map:
            return data_type_map[data_type]

        hint_map = [
            ("COD", "COD_"),
            ("ID", "COD_"),
            ("NOM", "NOM_"),
            ("NOME", "NOM_"),
            ("QTD", "QTD_"),
            ("QTDE", "QTD_"),
            ("QUANT", "QTD_"),
            ("DATA", "DAT_"),
            ("DT", "DAT_"),
            ("HORA", "HOR_"),
            ("HR", "HOR_"),
            ("SIT", "SIT_"),
            ("STATUS", "STA_"),
            ("DESC", "DSC_"),
            ("DESCR", "DSC_"),
            ("TXT", "TXT_"),
            ("VALOR", "VLR_"),
            ("VL", "VLR_"),
            ("TOTAL", "TOT_"),
            ("TIPO", "TIP_"),
            ("SEQ", "SEQ_"),
        ]
        for token, prefix in hint_map:
            if token in base_upper:
                return prefix

        return "TXT_"

    def _abbreviate(self, name: str) -> str:
        abbreviations = {
            "DATA": "DAT",
            "INFORMACAO": "INF",
            "INFORMACOES": "INF",
            "QUANTIDADE": "QTD",
            "NUMERO": "NUM",
            "DESCRICAO": "DSC",
            "CATEGORIA": "CAT",
            "REFERENCIA": "REF",
            "DOCUMENTO": "DOC",
            "PROCESSO": "PRO",
            "CODIGO": "COD",
            "HISTORICO": "HIS",
            "PERCENTUAL": "PER",
            "SITUACAO": "SIT",
            "STATUS": "STA",
        }
        parts = name.upper().split("_")
        new_parts = [abbreviations.get(p, p) for p in parts if p]
        return "_".join(new_parts)

    def _table_context(self, table: str) -> str:
        if not table:
            return "evento"
        parts = [p.lower() for p in table.split("_") if p]
        return " ".join(parts) if parts else "evento"

    def _suggest_table_comment(self, owner: str, table: str) -> Tuple[str, str, float]:
        context = self.table_context_lookup.get((owner, table), {})
        if not context:
            return "", "LLM_CONTEXT_MISSING", 0.0
        llm_comment = self.llm_comment_suggester.suggest_table_comment(context)
        if not llm_comment:
            source = self._resolve_llm_failure_source()
            return "", source, 0.0
        return llm_comment, "LLM", 0.9

    def _resolve_llm_failure_source(self) -> str:
        if not self.llm_comment_suggester.enabled:
            return getattr(self.llm_comment_suggester, "disabled_reason", "LLM_DISABLED") or "LLM_DISABLED"
        last_error = getattr(self.llm_comment_suggester, "last_error", "")
        if last_error:
            return "LLM_ERROR"
        return "LLM_NO_RESULT"

    def _resolve_llm_failure_detail(self, source: str) -> str:
        if source != "LLM_ERROR":
            return ""
        detail = getattr(self.llm_comment_suggester, "last_error", "") or ""
        detail = re.sub(r"\s+", " ", str(detail)).strip()
        if len(detail) > 300:
            detail = detail[:297].rstrip() + "..."
        return detail
