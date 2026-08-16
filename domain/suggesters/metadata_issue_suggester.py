from __future__ import annotations

import json
import os
import httpx
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import inflect
import pandas as pd

from dataquality.domain.config.llm_comment_config import LLMCommentConfig
from dataquality.domain.config.validation_config import ValidationConfig


def _read_keyring(service: str, username: str) -> str:
    if not service or not username:
        return ""
    try:
        import keyring
        return keyring.get_password(service, username) or ""
    except Exception:
        return ""


def _build_proxy_http_client() -> Optional["httpx.Client"]:
    """Build an httpx client that authenticates against a corporate proxy, if configured.

    The proxy itself is detected the same way urllib does (env vars first,
    falling back to the OS-level config on Windows/macOS), because this
    network's proxy is configured system-wide and is not exposed through
    HTTPS_PROXY/HTTP_PROXY.

    Proxy credentials are embedded as Basic auth directly in the proxy URL.
    That is a deliberate choice, not the simplest option: httpcore only ever
    sends Proxy-Authorization on the initial CONNECT when credentials are
    embedded in the proxy URL (Basic). An `auth=` object such as
    httpx_ntlm's HttpNtlmAuth authenticates the *request* that flows through
    an already-established tunnel — it never gets a chance to run if the
    CONNECT itself is rejected with 407, which is the failure seen here.
    """
    from urllib.request import getproxies

    proxy_info = getproxies()
    proxy_url = (
        os.environ.get("HTTPS_PROXY")
        or os.environ.get("HTTP_PROXY")
        or proxy_info.get("https")
        or proxy_info.get("http")
    )
    if not proxy_url:
        return None

    proxy_user = os.environ.get("PROXY_USER", "")
    proxy_pass = os.environ.get("PROXY_PASS", "")
    if proxy_user and not proxy_pass:
        proxy_pass = _read_keyring("dataquality-proxy", proxy_user)

    if proxy_user and proxy_pass:
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(proxy_url)
        if not parsed.username:
            netloc = f"{proxy_user}:{proxy_pass}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            proxy_url = urlunparse(parsed._replace(netloc=netloc))

    return httpx.Client(proxy=proxy_url)


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
    api_type: str = "openai"
    api_version: str = ""
    disabled_reason: str = ""
    last_error: str = ""
    response_cache: dict[str, Optional[str]] = field(default_factory=dict)

    @classmethod
    def from_config(cls, config: LLMCommentConfig) -> "OpenAICompatibleCommentSuggester":
        api_key = os.getenv(config.api_key_env, "") if getattr(config, "api_key_env", "") else ""
        if not api_key:
            api_key = _read_keyring(
                getattr(config, "api_key_keyring_service", ""),
                getattr(config, "api_key_keyring_username", ""),
            )
        requires_api_key = bool(getattr(config, "require_api_key", True))
        enabled = bool(config.enabled and config.model and (api_key or not requires_api_key))
        if not config.enabled:
            disabled_reason = "LLM_DISABLED"
        elif requires_api_key and not api_key:
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
            api_type=getattr(config, "api_type", "openai"),
            api_version=getattr(config, "api_version", ""),
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
            "Você é um assistente especializado em documentação de dados."
            "O comentario deve ter uma unica frase, em portugues, com foco de negocio, objetivo e claro."
            "Utilize um estilo profissional de dicionário de dados."
            "Seja específico, porém conciso. Prefira uma sentença."
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

    def _build_endpoint(self) -> str:
        base = self.base_url.rstrip("/")
        api_version = getattr(self, "api_version", "")
        if getattr(self, "api_type", "openai").lower() == "azure":
            # Azure format: {endpoint}/openai/deployments/{model}/chat/completions?api-version=...
            url = f"{base}/openai/deployments/{self.model}/chat/completions"
            if api_version:
                url += f"?api-version={api_version}"
            return url
        return base + "/chat/completions"

    def _post_json(self, payload: Dict[str, Any]) -> str | None:
        http_client = self._build_http_client()
        try:
            self.last_error = ""
            endpoint = self._build_endpoint()
            owns_client = http_client is None
            client = http_client or httpx.Client()
            try:
                response = client.post(
                    endpoint,
                    json=payload,
                    headers=self._build_headers(),
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                body = response.json()
            finally:
                if owns_client:
                    client.close()
        except httpx.HTTPStatusError as exc:
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

    def _format_http_error(self, exc: "httpx.HTTPStatusError") -> str:
        detail = ""
        try:
            raw = exc.response.text
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
        summary = f"HTTP {exc.response.status_code} {exc.response.reason_phrase}".strip()
        return f"{summary}: {detail}".strip(": ")

    def _build_http_client(self) -> Optional["httpx.Client"]:
        return _build_proxy_http_client()

    def _build_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            if getattr(self, "api_type", "openai").lower() == "azure":
                headers["api-key"] = self.api_key
            else:
                headers["Authorization"] = f"Bearer {self.api_key}"
        return headers


@dataclass
class AnthropicCommentSuggester(LLMCommentSuggester):
    api_key: str = ""
    model: str = "claude-sonnet-4-6"
    business_context: Dict[str, Any] = field(default_factory=dict)
    # Vision/requirements/use-case/business-rule documents extracted from
    # .odt/.docx/.pdf (business_docs_context_<schema>.json), distinct from
    # business_context (which comes from source code). Optional -- most
    # schemas won't have this file yet.
    business_docs_context: Dict[str, Any] = field(default_factory=dict)
    metadata_fallback: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 60
    temperature: float = 0.2
    max_output_tokens: int = 512
    disabled_reason: str = ""
    last_error: str = ""
    response_cache: Dict[str, Optional[str]] = field(default_factory=dict)

    @classmethod
    def from_config(
        cls,
        config: LLMCommentConfig,
        context_path: Optional[Path] = None,
        metadata_fallback: Optional[Dict[str, Any]] = None,
        business_docs_context_path: Optional[Path] = None,
    ) -> "AnthropicCommentSuggester":
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key and getattr(config, "api_key_env", ""):
            api_key = os.getenv(config.api_key_env, "")
        if not api_key:
            api_key = _read_keyring(
                getattr(config, "api_key_keyring_service", ""),
                getattr(config, "api_key_keyring_username", ""),
            )

        requires_api_key = bool(getattr(config, "require_api_key", True))
        enabled = bool(config.enabled and config.model and (api_key or not requires_api_key))

        if not config.enabled:
            disabled_reason = "LLM_DISABLED"
        elif requires_api_key and not api_key:
            disabled_reason = "LLM_MISSING_API_KEY"
        elif not config.model:
            disabled_reason = "LLM_MISSING_MODEL"
        else:
            disabled_reason = ""

        business_context: Dict[str, Any] = {}
        if context_path and Path(context_path).exists():
            try:
                with open(context_path, encoding="utf-8") as f:
                    business_context = json.load(f)
            except Exception:
                pass

        business_docs_context: Dict[str, Any] = {}
        if business_docs_context_path and Path(business_docs_context_path).exists():
            try:
                with open(business_docs_context_path, encoding="utf-8") as f:
                    business_docs_context = json.load(f)
            except Exception:
                pass

        return cls(
            enabled=enabled,
            api_key=api_key,
            model=config.model,
            business_context=business_context,
            business_docs_context=business_docs_context,
            metadata_fallback=metadata_fallback or {},
            timeout_seconds=int(config.timeout_seconds),
            temperature=float(config.temperature),
            max_output_tokens=int(config.max_output_tokens),
            disabled_reason=disabled_reason,
        )

    def suggest_column_comment(self, context: Dict[str, Any]) -> Optional[str]:
        return self._suggest(context, entity_type="column")

    def suggest_table_comment(self, context: Dict[str, Any]) -> Optional[str]:
        return self._suggest(context, entity_type="table")

    def _suggest(self, context: Dict[str, Any], entity_type: str) -> Optional[str]:
        if not self.enabled:
            return None

        try:
            import anthropic as _anthropic
        except ImportError:
            self.last_error = "anthropic package not installed. Run: pip install anthropic"
            return None

        cache_key = json.dumps(
            {"entity_type": entity_type, "ctx": {k: str(v)[:120] for k, v in context.items()}},
            ensure_ascii=False, sort_keys=True,
        )
        if cache_key in self.response_cache:
            return self.response_cache[cache_key]

        table_name = str(context.get("table_name", ""))
        business_ctx = self._filter_business_context(table_name)
        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(context, entity_type, business_ctx)

        try:
            self.last_error = ""
            http_client = self._build_http_client()
            client = _anthropic.Anthropic(
                api_key=self.api_key,
                **({"http_client": http_client} if http_client is not None else {}),
            )
            response = client.messages.create(
                model=self.model,
                max_tokens=self.max_output_tokens,
                temperature=self.temperature,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            raw = response.content[0].text if response.content else ""
        except Exception as exc:
            self.last_error = str(exc)
            print(f"[AnthropicCommentSuggester] ERRO: {self.last_error}")
            self.response_cache[cache_key] = None
            return None

        comment = self._extract_comment(raw)
        self.response_cache[cache_key] = comment
        return comment

    def _build_http_client(self):
        return _build_proxy_http_client()

    def _filter_business_context(self, table_name: str) -> Dict[str, Any]:
        if not self.business_context:
            return self._filter_metadata_fallback(table_name)

        table_lower = table_name.lower().replace("_", "")
        all_dtos = self.business_context.get("dtos_entrada", []) + self.business_context.get("dtos_saida", [])
        relevant_dtos = [
            dto for dto in all_dtos
            if table_lower in str(dto.get("operacao", "")).lower().replace("_", "")
        ][:3]
        all_enums = self.business_context.get("enumeracoes", [])
        relevant_enums = [e for e in all_enums if e.get("valores")][:5]

        result: Dict[str, Any] = {}
        if relevant_dtos:
            result["dtos"] = relevant_dtos
        if relevant_enums:
            result["enums"] = relevant_enums
        relevant_docs = self._match_business_docs(table_name)
        if relevant_docs:
            result["business_docs"] = relevant_docs
        return result

    def _match_business_docs(self, table_name: str) -> list[Dict[str, Any]]:
        """Match requisitos/casos de uso/regras de negocio documents
        (business_docs_context) to a table by token overlap between the table
        name and the document's titulo/resumo/requisitos/regras_negocio --
        same approach DenodoCatalogInputBuilder._match_business_docs uses,
        kept local here since the two projects don't share code."""
        documentos = self.business_docs_context.get("documentos_negocio", [])
        if not isinstance(documentos, list) or not documentos:
            return []

        table_tokens = {token for token in re.split(r"[^A-Z0-9]+", table_name.upper()) if len(token) > 2}
        if not table_tokens:
            return []

        relevant_doc_types = {"caso_de_uso", "regras_negocio", "requisitos"}
        scored: list[tuple] = []
        for doc in documentos:
            if not isinstance(doc, dict) or doc.get("tipo_documento") not in relevant_doc_types:
                continue
            haystack = json.dumps(
                {key: doc.get(key) for key in ("titulo", "resumo", "requisitos", "regras_negocio")},
                ensure_ascii=False,
            ).upper()
            score = sum(1 for token in table_tokens if token in haystack)
            if score > 0:
                scored.append((score, {
                    "titulo": doc.get("titulo"),
                    "resumo": doc.get("resumo"),
                }))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [entry for _, entry in scored[:3]]

    def _filter_metadata_fallback(self, table_name: str) -> Dict[str, Any]:
        if not self.metadata_fallback:
            return {}
        table_upper = table_name.upper()
        table_entry = next(
            (t for t in self.metadata_fallback.get("tables", []) if str(t.get("table_name", "")).upper() == table_upper),
            None,
        )
        if not table_entry:
            return {}
        result: Dict[str, Any] = {}
        existing = table_entry.get("existing_column_comments")
        if existing:
            result["existing_column_comments"] = existing
        keywords = table_entry.get("column_name_keywords")
        if keywords:
            result["column_name_keywords"] = keywords
        related = table_entry.get("related_tables")
        if related:
            result["related_tables"] = related
        return result

    def _build_system_prompt(self) -> str:
        schema_desc = str(self.business_context.get("descricao", "")).strip()
        vision_summary = self._build_vision_summary()
        intro = ""
        if schema_desc:
            intro += f"Contexto do sistema: {schema_desc}\n\n"
        if vision_summary:
            intro += f"Visao do sistema: {vision_summary}\n\n"
        return (
            "Voce e um especialista em banco de dados Oracle e no dominio fiscal/tributario "
            "da Secretaria da Fazenda do Estado do Ceara (Sefaz-CE).\n"
            f"{intro}"
            "Regras:\n"
            "- Comentario de tabela: 1-2 frases descrevendo o proposito da tabela.\n"
            "- Comentario de coluna: 1 frase clara e objetiva sobre o significado de negocio.\n"
            "- Para colunas COD_: inclua os valores possiveis se disponiveis no contexto.\n"
            "- Para colunas DAT_: indique o evento que a data registra.\n"
            "- Nao use acentos (compatibilidade Oracle < 23c).\n"
            "- Nao invente significados alem do contexto fornecido.\n"
            'Responda SOMENTE com JSON valido no formato {"comment": "..."}.'
        )

    def _build_vision_summary(self) -> str:
        """Schema-wide narrative from business_docs_context's tipo_documento="visao"
        entries -- same construction as DenodoCatalogInputBuilder._build_vision_summary."""
        documentos = self.business_docs_context.get("documentos_negocio", [])
        if not isinstance(documentos, list):
            return ""
        parts: list[str] = []
        for doc in documentos:
            if not isinstance(doc, dict) or doc.get("tipo_documento") != "visao":
                continue
            resumo = str(doc.get("resumo", "")).strip()
            if resumo:
                parts.append(resumo)
        return " ".join(parts)

    def _build_user_prompt(self, context: Dict[str, Any], entity_type: str, business_ctx: Dict[str, Any]) -> str:
        if entity_type == "column":
            parts = [
                f"Gere um comentario Oracle para a coluna abaixo.",
                f"TABELA: {context.get('table_name', '')}",
                f"COLUNA: {context.get('column_name', '')} ({context.get('data_type', '')})",
                f"PK: {context.get('is_pk', False)} | FK: {context.get('is_fk', False)} | NULLABLE: {context.get('nullable', '')}",
            ]
            if context.get("table_comment"):
                parts.append(f"COMENTARIO DA TABELA: {context['table_comment']}")
            if context.get("references"):
                ref = context["references"]
                parts.append(f"REFERENCIA: {ref.get('table', '')} ({ref.get('column', '')})")
        else:
            parts = [
                f"Gere um comentario Oracle para a tabela abaixo.",
                f"TABELA: {context.get('table_name', '')}",
                f"TIPO INFERIDO: {context.get('table_type_inference', '')}",
                f"COLUNAS PRINCIPAIS: {', '.join(context.get('main_columns', [])[:8])}",
            ]
            if context.get("row_count"):
                parts.append(f"TOTAL DE LINHAS: {context['row_count']}")

        if business_ctx:
            parts.append(f"\nCONTEXTO DE NEGOCIO:\n{json.dumps(business_ctx, ensure_ascii=False, indent=2)}")

        return "\n".join(parts)

    def _extract_comment(self, raw: str) -> Optional[str]:
        match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        candidate = match.group(0) if match else raw
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            payload = {"comment": raw.strip()}
        comment = str(payload.get("comment", "")).strip()
        return comment or None


class MetadataIssueSuggester:
    def __init__(
        self,
        db_type: str = "Oracle",
        config: Optional[ValidationConfig] = None,
        llm_comment_suggester: Optional[LLMCommentSuggester] = None,
        schema_context: Optional[Dict[str, Any]] = None,
        comment_generation_strategy: str = "rules",
    ):
        self.db_type = (db_type or "").strip()
        self.config = config or ValidationConfig()
        self.llm_comment_suggester = llm_comment_suggester or LLMCommentSuggester(enabled=False)
        self.schema_context = schema_context or {}
        self.comment_generation_strategy = (comment_generation_strategy or "rules").strip().lower()
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
        for col in (
            "COLUMN_TYPE",
            "SUGGESTED_VALUE_RULES",
            "SUGGESTED_SOURCE",
            "SUGGESTED_CONFIDENCE",
            "SUGGESTED_VALUE_LLM",
        ):
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

        # suggested_value already IS the rules-based suggestion for every rule
        # (comment rules included: _suggest_column_comment/_suggest_table_comment
        # route to *_by_rules when comment_generation_strategy != "llm"), so it's
        # reported directly as SUGGESTED_VALUE_RULES -- no separate rules_value
        # recomputation needed. SUGGESTED_VALUE_LLM stays a comparison-only
        # column, populated for the comment rules (MQME008/027) whenever the LLM
        # suggester is enabled, regardless of the primary strategy.
        suggested_value_llm = ""

        if rule == "MQME008":
            context = self.column_context_lookup.get((owner, table, column), {})
            if self.llm_comment_suggester.enabled:
                llm_value, _, _ = self._suggest_column_comment_via_llm(context)
                suggested_value_llm = llm_value
        elif rule == "MQME027":
            context = self.table_context_lookup.get((owner, table), {})
            if self.llm_comment_suggester.enabled:
                llm_value, _, _ = self._suggest_table_comment_via_llm(context)
                suggested_value_llm = llm_value

        return {
            "COLUMN_TYPE": column_type,
            "SUGGESTED_VALUE_RULES": suggested_value,
            "SUGGESTED_SOURCE": source,
            "SUGGESTED_CONFIDENCE": confidence,
            "SUGGESTED_VALUE_LLM": suggested_value_llm,
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
        if self.comment_generation_strategy != "llm":
            return self._suggest_column_comment_by_rules(owner, table, column, context)
        return self._suggest_column_comment_via_llm(context)

    def _suggest_column_comment_via_llm(self, context: Dict[str, Any]) -> Tuple[str, str, float]:
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
        if self.comment_generation_strategy != "llm":
            return self._suggest_table_comment_by_rules(owner, table, context)
        return self._suggest_table_comment_via_llm(context)

    def _suggest_table_comment_via_llm(self, context: Dict[str, Any]) -> Tuple[str, str, float]:
        if not context:
            return "", "LLM_CONTEXT_MISSING", 0.0
        llm_comment = self.llm_comment_suggester.suggest_table_comment(context)
        if not llm_comment:
            source = self._resolve_llm_failure_source()
            return "", source, 0.0
        return llm_comment, "LLM", 0.9

    def _suggest_column_comment_by_rules(
        self,
        owner: str,
        table: str,
        column: str,
        context: Dict[str, Any],
    ) -> Tuple[str, str, float]:
        if not column:
            return "", "", 0.0
        references = context.get("references", {}) if isinstance(context, dict) else {}
        if not isinstance(references, dict):
            references = {}
        ref_table = self._clean_str(references.get("table", "")).upper()
        ref_column = self._clean_str(references.get("column", "")).upper()
        table_comment = self._clean_str(context.get("table_comment", "")) if isinstance(context, dict) else ""

        base_tokens = self._meaningful_tokens(column)
        all_tokens = self._all_tokens(column)
        entity_label = self._friendly_identifier(column)
        table_label = self._friendly_identifier(table)
        if ref_table:
            ref_label = self._friendly_identifier(ref_table)
            ref_col_label = self._friendly_identifier(ref_column or column)
            return (
                f"Identificador de {ref_col_label} relacionado ao registro de {ref_label}.",
                "RULES",
                0.75,
            )
        if self._has_any_token(all_tokens, {"DAT", "DATA"}):
            return (f"Data associada ao registro de {table_label}.", "RULES", 0.72)
        if self._has_any_token(all_tokens, {"HORA", "HOR", "HR"}):
            return (f"Horário associado ao registro de {table_label}.", "RULES", 0.72)
        if self._has_any_token(all_tokens, {"VLR", "VALOR", "TOTAL", "TOT"}):
            return (f"Valor registrado para {entity_label} do registro.", "RULES", 0.72)
        if self._has_any_token(all_tokens, {"QTD", "QTDE", "QUANTIDADE", "QUANT"}):
            return (f"Quantidade registrada para {entity_label} no contexto de {table_label}.", "RULES", 0.72)
        if self._has_any_token(all_tokens, {"COD", "ID", "SEQ", "NUM"}):
            return (f"Código que identifica {entity_label} no contexto de {table_label}.", "RULES", 0.7)
        if self._has_any_token(all_tokens, {"TIP", "TIPO"}):
            return (f"Tipo de {entity_label} associado ao registro.", "RULES", 0.7)
        if self._has_any_token(all_tokens, {"SIT", "STATUS", "STA"}):
            return (f"Situação de {entity_label} associada ao registro.", "RULES", 0.7)
        if self._has_any_token(all_tokens, {"NOM", "NOME"}):
            return (f"Nome de {entity_label} associado ao registro.", "RULES", 0.7)
        if self._has_any_token(all_tokens, {"DSC", "DESC", "DESCRICAO", "DESCR"}):
            return (f"Descrição de {entity_label} associada ao registro.", "RULES", 0.7)
        if table_comment:
            return (f"Informação de {entity_label} relacionada a {self._sentence_case(table_comment)}", "RULES", 0.68)
        return (f"Informação de {entity_label} associada ao registro de {table_label}.", "RULES", 0.65)

    def _suggest_table_comment_by_rules(
        self,
        owner: str,
        table: str,
        context: Dict[str, Any],
    ) -> Tuple[str, str, float]:
        if not table:
            return "", "", 0.0
        context = context or {}
        table_type = self._clean_str(context.get("table_type_inference", "")).lower()
        related_tables = context.get("related_tables", []) if isinstance(context.get("related_tables", []), list) else []
        main_columns = context.get("main_columns", []) if isinstance(context.get("main_columns", []), list) else []
        table_label = self._friendly_identifier(table)

        if table_type == "log":
            return (f"Tabela de histórico ou auditoria dos eventos relacionados a {table_label}.", "RULES", 0.76)
        if table_type == "referencia":
            return (f"Tabela de referência para cadastro e classificação de {table_label}.", "RULES", 0.76)
        if table_type == "associacao":
            return (f"Tabela de associação entre entidades relacionadas a {table_label}.", "RULES", 0.76)
        if table_type == "transacao":
            return (f"Tabela para registro das transações ou movimentações de {table_label}.", "RULES", 0.76)
        if table_type == "detalhe":
            return (f"Tabela de detalhamento das informações de {table_label}.", "RULES", 0.76)
        if related_tables:
            related_label = self._friendly_identifier(str(related_tables[0]))
            return (f"Tabela com informações de {table_label} relacionadas a {related_label}.", "RULES", 0.72)
        if main_columns:
            main_label = self._friendly_identifier(str(main_columns[0]))
            return (f"Tabela para armazenamento das informações de {table_label}, com destaque para {main_label}.", "RULES", 0.7)
        return (f"Tabela para armazenamento das informações de {table_label}.", "RULES", 0.68)

    def _resolve_llm_failure_source(self) -> str:
        if not self.llm_comment_suggester.enabled:
            return getattr(self.llm_comment_suggester, "disabled_reason", "LLM_DISABLED") or "LLM_DISABLED"
        last_error = getattr(self.llm_comment_suggester, "last_error", "")
        if last_error:
            return "LLM_ERROR"
        return "LLM_NO_RESULT"

    def _meaningful_tokens(self, value: str) -> list[str]:
        stop_tokens = {"DE", "DA", "DO", "DOS", "DAS", "R", "TAB", "TBL"}
        prefix_tokens = {"COD", "DAT", "DSC", "NOM", "NUM", "QTD", "SEQ", "SIT", "STA", "TXT", "TIP", "TOT", "VLR", "BIN", "HOR", "XML", "ID"}
        tokens = self._all_tokens(value)
        return [token for token in tokens if token not in stop_tokens and token not in prefix_tokens]

    def _has_any_token(self, tokens: list[str], expected: set[str]) -> bool:
        return any(token in expected for token in tokens)

    def _all_tokens(self, value: str) -> list[str]:
        return [token for token in re.split(r"[^A-Z0-9]+", str(value).upper()) if token]

    def _friendly_identifier(self, value: str) -> str:
        tokens = self._meaningful_tokens(value)
        if not tokens:
            tokens = [token for token in re.split(r"[^A-Z0-9]+", str(value).upper()) if token]
        friendly = " ".join(token.lower() for token in tokens if token)
        return friendly or str(value).strip().lower()

    def _sentence_case(self, value: str) -> str:
        text = self._clean_str(value)
        if not text:
            return ""
        return text[0].lower() + text[1:] if len(text) > 1 else text.lower()
