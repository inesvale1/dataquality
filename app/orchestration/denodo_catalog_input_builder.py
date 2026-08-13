from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from dataquality.infrastructure.io.pipeline_bridge import ensure_business_docs_context, ensure_metadata_context, ensure_sources_context
from dataquality.domain.config.validation_config import ValidationConfig

_CURATION_STATUS_PENDING = "PENDING_CURATION"
_MAX_MATCHED_DTOS = 3
_MAX_MATCHED_BUSINESS_RULES = 3
_MAX_MATCHED_BUSINESS_DOCS = 3
# documentos_negocio entries worth matching against a specific table: a whole
# "visao" document is schema-wide (see business_vision_summary below), not
# table-specific, so it's excluded here on purpose.
_TABLE_RELEVANT_DOC_TYPES = {"caso_de_uso", "regras_negocio", "requisitos"}

# Subset of ValidationConfig.type_naming.identifier_name_patterns tokens that are
# genuinely indicative of personal/sensitive data (matches the "Sensibilidade
# tecnica" example fields in the Denodo catalog proposal: CPF, CNPJ, endereco,
# telefone, e-mail). ID/COD/PROTOCOLO/UF are excluded here: they exist in that
# same config list for naming-convention checks (MQID rules), not for PII
# detection, and would flag almost every code column as sensitive.
_PERSONAL_DATA_TOKENS = {"CPF", "CNPJ", "CEP", "EMAIL", "PLACA", "RENAVAM", "CHASSI", "MATRICULA"}

# Priority order when reading the already-computed METADATA_SCORES DataFrame:
# a combined SCHEMA_SCORE is only present when both model-quality and
# data-quality phases ran together; fall back to whichever phase is available.
_QUALITY_SCORE_COMPONENT_PRIORITY = ("SCHEMA_SCORE", "MDDQ", "DDQ")


@dataclass
class DenodoCatalogInputBuilder:
    """Builds a canonical Denodo Data Catalog input document for one schema.

    Combines three sources that already exist in the framework, without adding
    any new extraction or LLM step of its own:
    - technical context read from `metadata_context_<schema>.json` (built by the
      sibling `technicalcatalogpipeline` project; requested on demand via
      `pipeline_bridge.ensure_metadata_context` if missing and required);
    - business context read from `sources_context_<schema>.json` (built by the
      sibling `businessglossarypipeline` project; requested on demand via
      `pipeline_bridge.ensure_sources_context` if missing and required) --
      Java source scan output: regras_negocio, enumeracoes, dtos_entrada/saida,
      tabelas_sql;
    - business documents context read from `business_docs_context_<schema>.json`
      (also built by `businessglossarypipeline`, via `--only docs`; requested
      on demand via `pipeline_bridge.ensure_business_docs_context` if missing
      and required) -- vision/requirements/use-case/business-rule documents
      extracted from .odt/.docx/.pdf, optional and off by default (see
      require_business_docs_context);
    - the schema quality score already computed by `domain.scoring.quality_scorer`.

    Fields that require synthesis (business_name, business_description, business_domain
    per table) are left as `None`/candidate lists for curatorship or a later LLM step,
    per the "Nivel 2 - governed catalog" workflow described in the Denodo proposal docs.
    """

    schema_name: str
    inputs_dir: Path
    output_dir: Path
    workspace_root: Path
    require_metadata_context: bool = True
    require_sources_context: bool = False
    require_business_docs_context: bool = False
    business_context: dict[str, Any] | None = None
    business_docs_context: dict[str, Any] | None = None
    quality_scores_df: pd.DataFrame | None = None
    validation_config: ValidationConfig = field(default_factory=ValidationConfig)
    source_system: str | None = None

    def build_and_save(self, payload: dict[str, Any] | None = None) -> Path:
        payload = payload or self.build()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.output_dir / f"denodo_catalog_input_{self.schema_name}.json"
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return output_path

    def build(self) -> dict[str, Any]:
        technical_context = ensure_metadata_context(
            schema_name=self.schema_name,
            inputs_dir=self.inputs_dir,
            required=self.require_metadata_context,
            workspace_root=self.workspace_root,
        )

        if self.business_context is not None:
            business_context = self.business_context
        else:
            business_context = ensure_sources_context(
                schema_name=self.schema_name,
                inputs_dir=self.inputs_dir,
                required=self.require_sources_context,
                workspace_root=self.workspace_root,
            ) or {}

        if self.business_docs_context is not None:
            business_docs_context = self.business_docs_context
        else:
            business_docs_context = ensure_business_docs_context(
                schema_name=self.schema_name,
                inputs_dir=self.inputs_dir,
                required=self.require_business_docs_context,
                workspace_root=self.workspace_root,
            ) or {}

        columns_by_table = self._group_columns_by_table(technical_context.get("columns", []))
        sensitive_patterns = self._compile_sensitive_patterns()
        schema_quality_score, quality_score_source = self._resolve_schema_quality_score()
        referenced_tables = self._normalize_referenced_tables(business_context)

        tables = [
            self._build_table_entry(
                table_context,
                columns_by_table.get(str(table_context.get("table_name", "")), []),
                business_context,
                business_docs_context,
                sensitive_patterns,
                schema_quality_score,
                referenced_tables,
            )
            for table_context in technical_context.get("tables", [])
        ]

        return {
            "source_system": self.source_system or self.schema_name,
            "schema_name": self.schema_name,
            "db_instance_name": technical_context.get("db_instance_name"),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "curation_status": _CURATION_STATUS_PENDING,
            "business_domain_hint": self._infer_business_domain(business_context),
            "business_vision_summary": self._build_vision_summary(business_docs_context),
            "business_glossary": self._build_glossary(business_context),
            "quality_score": schema_quality_score,
            "quality_score_source": quality_score_source,
            "quality_score_granularity": "schema" if schema_quality_score is not None else None,
            "tables": tables,
        }

    # ------------------------------------------------------------------
    # Table / column assembly
    # ------------------------------------------------------------------

    def _build_table_entry(
        self,
        table_context: dict[str, Any],
        column_contexts: list[dict[str, Any]],
        business_context: dict[str, Any],
        business_docs_context: dict[str, Any],
        sensitive_patterns: list[tuple[str, re.Pattern[str]]],
        schema_quality_score: float | None,
        referenced_tables: set[str],
    ) -> dict[str, Any]:
        owner = str(table_context.get("owner", ""))
        table_name = str(table_context.get("table_name", ""))
        existing_column_comments = table_context.get("existing_column_comments", {})

        columns = [
            self._build_column_entry(column_context, existing_column_comments, sensitive_patterns)
            for column_context in column_contexts
        ]
        contains_personal_data = any(column["contains_personal_data"] for column in columns)
        qualified_name = f"{owner}.{table_name}".upper()

        return {
            "source_schema": owner,
            "source_table": table_name,
            "db_instance_name": table_context.get("db_instance_name"),
            "denodo_database": None,
            "denodo_view": None,
            "business_name": None,
            "business_description": table_context.get("table_comment") or None,
            "business_domain": self._infer_business_domain(business_context),
            "table_type_inference": table_context.get("table_type_inference"),
            "primary_keys": table_context.get("primary_keys", []),
            "foreign_keys": table_context.get("foreign_keys", []),
            "related_tables": table_context.get("related_tables", []),
            "row_count": table_context.get("row_count"),
            "referenced_in_source_code": qualified_name in referenced_tables,
            "contains_personal_data": contains_personal_data,
            "classification": "Restrito" if contains_personal_data else "Nao classificado",
            "quality_score": schema_quality_score,
            "business_context_candidates": {
                "dtos": self._match_dtos(table_name, business_context),
                "business_rules": self._match_business_rules(table_name, business_context),
                "business_docs": self._match_business_docs(table_name, business_docs_context),
            },
            "columns": columns,
        }

    def _build_column_entry(
        self,
        column_context: dict[str, Any],
        existing_column_comments: dict[str, str],
        sensitive_patterns: list[tuple[str, re.Pattern[str]]],
    ) -> dict[str, Any]:
        column_name = str(column_context.get("column_name", ""))
        sensitive_pattern = self._match_sensitive_pattern(column_name, sensitive_patterns)

        return {
            "column_name": column_name,
            "denodo_column": None,
            "data_type": column_context.get("data_type"),
            "nullable": column_context.get("nullable"),
            "is_pk": column_context.get("is_pk", False),
            "is_fk": column_context.get("is_fk", False),
            "is_uk": column_context.get("is_uk", False),
            "references": column_context.get("references") or {},
            "business_name": None,
            "business_description": existing_column_comments.get(column_name) or None,
            "contains_personal_data": sensitive_pattern is not None,
            "sensitive_pattern": sensitive_pattern,
        }

    def _group_columns_by_table(self, columns: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for column_context in columns:
            table_name = str(column_context.get("table_name", ""))
            grouped.setdefault(table_name, []).append(column_context)
        return grouped

    # ------------------------------------------------------------------
    # Personal-data heuristic (reuses ValidationConfig.type_naming, the
    # same name-pattern list already used for MQID naming-consistency checks;
    # br_documents.py only validates CPF/CNPJ/CGF *values*, not column names,
    # so it is not applicable here without sampling actual data).
    # ------------------------------------------------------------------

    def _compile_sensitive_patterns(self) -> list[tuple[str, re.Pattern[str]]]:
        patterns: list[tuple[str, re.Pattern[str]]] = []
        for raw_pattern in self.validation_config.type_naming.identifier_name_patterns:
            tokens = re.findall(r"[A-Z]+", raw_pattern)
            label = "_".join(tokens) if tokens else raw_pattern
            if label not in _PERSONAL_DATA_TOKENS:
                continue
            patterns.append((label, re.compile(raw_pattern, flags=re.IGNORECASE)))
        return patterns

    def _match_sensitive_pattern(
        self, column_name: str, sensitive_patterns: list[tuple[str, re.Pattern[str]]]
    ) -> str | None:
        upper_name = column_name.upper()
        for label, pattern in sensitive_patterns:
            if pattern.search(upper_name):
                return label
        return None

    # ------------------------------------------------------------------
    # Business context (from sources_context_<schema>.json)
    # ------------------------------------------------------------------

    def _infer_business_domain(self, business_context: dict[str, Any]) -> str:
        sistema = str(business_context.get("sistema", "")).strip()
        if sistema:
            return sistema.title()
        return self.schema_name.title()

    def _build_glossary(self, business_context: dict[str, Any]) -> list[dict[str, Any]]:
        enumeracoes = business_context.get("enumeracoes", [])
        if not isinstance(enumeracoes, list):
            return []
        glossary = []
        for item in enumeracoes:
            if not isinstance(item, dict):
                continue
            glossary.append(
                {
                    "termo": item.get("nome"),
                    "pacote": item.get("pacote"),
                    "valores": item.get("valores", []),
                }
            )
        return glossary

    def _normalize_referenced_tables(self, business_context: dict[str, Any]) -> set[str]:
        tabelas_sql = business_context.get("tabelas_sql", [])
        if not isinstance(tabelas_sql, list):
            return set()
        return {str(table).strip().upper() for table in tabelas_sql if str(table).strip()}

    def _match_dtos(self, table_name: str, business_context: dict[str, Any]) -> list[dict[str, Any]]:
        table_token = self._normalize_token(table_name)
        if not table_token:
            return []
        all_dtos = list(business_context.get("dtos_entrada", [])) + list(business_context.get("dtos_saida", []))
        matches = [
            dto
            for dto in all_dtos
            if isinstance(dto, dict) and table_token in self._normalize_token(str(dto.get("operacao", "")))
        ]
        return matches[:_MAX_MATCHED_DTOS]

    def _match_business_rules(self, table_name: str, business_context: dict[str, Any]) -> list[dict[str, Any]]:
        table_tokens = set(re.split(r"[^A-Z0-9]+", table_name.upper()))
        table_tokens = {token for token in table_tokens if len(token) > 2}
        if not table_tokens:
            return []

        regras_negocio = business_context.get("regras_negocio", [])
        if not isinstance(regras_negocio, list):
            return []

        scored: list[tuple[int, dict[str, Any]]] = []
        for entry in regras_negocio:
            if not isinstance(entry, dict):
                continue
            haystack = json.dumps(entry, ensure_ascii=False).upper()
            score = sum(1 for token in table_tokens if token in haystack)
            if score > 0:
                scored.append((score, entry))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [entry for _, entry in scored[:_MAX_MATCHED_BUSINESS_RULES]]

    def _normalize_token(self, value: str) -> str:
        return re.sub(r"[^A-Z0-9]+", "", value.upper())

    # ------------------------------------------------------------------
    # Business context (from business_docs_context_<schema>.json -- vision/
    # requirements/use-case/business-rule documents, distinct from
    # sources_context which comes from source code)
    # ------------------------------------------------------------------

    def _build_vision_summary(self, business_docs_context: dict[str, Any]) -> str | None:
        """Schema-wide narrative (not matched to any one table), built from
        every documento_negocio classified as tipo_documento="visao" --
        typically 0 or 1 per system, but concatenates if more than one."""
        documentos = business_docs_context.get("documentos_negocio", [])
        if not isinstance(documentos, list):
            return None

        parts: list[str] = []
        for doc in documentos:
            if not isinstance(doc, dict) or doc.get("tipo_documento") != "visao":
                continue
            resumo = str(doc.get("resumo", "")).strip()
            if resumo:
                parts.append(resumo)
            objetivos = doc.get("objetivos_negocio", [])
            if isinstance(objetivos, list) and objetivos:
                parts.append("Objetivos: " + "; ".join(str(item) for item in objetivos))
        return " ".join(parts) if parts else None

    def _match_business_docs(self, table_name: str, business_docs_context: dict[str, Any]) -> list[dict[str, Any]]:
        """Match requisitos/casos de uso/regras de negocio documents to a
        table by token overlap between the table name and the document's
        titulo/resumo/requisitos/regras_negocio -- same approach as
        _match_business_rules, applied to a different source. Returns a
        compact reference (not the full document) since a matched doc can
        carry many requisitos/regras_negocio entries of its own."""
        documentos = business_docs_context.get("documentos_negocio", [])
        if not isinstance(documentos, list):
            return []

        table_tokens = set(re.split(r"[^A-Z0-9]+", table_name.upper()))
        table_tokens = {token for token in table_tokens if len(token) > 2}
        if not table_tokens:
            return []

        scored: list[tuple[int, dict[str, Any]]] = []
        for doc in documentos:
            if not isinstance(doc, dict) or doc.get("tipo_documento") not in _TABLE_RELEVANT_DOC_TYPES:
                continue
            haystack = json.dumps(
                {key: doc.get(key) for key in ("titulo", "resumo", "requisitos", "regras_negocio")},
                ensure_ascii=False,
            ).upper()
            score = sum(1 for token in table_tokens if token in haystack)
            if score > 0:
                scored.append((score, {
                    "arquivo": doc.get("arquivo"),
                    "tipo_documento": doc.get("tipo_documento"),
                    "titulo": doc.get("titulo"),
                    "resumo": doc.get("resumo"),
                }))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [entry for _, entry in scored[:_MAX_MATCHED_BUSINESS_DOCS]]

    # ------------------------------------------------------------------
    # Quality score (reuses whatever METADATA_SCORES-shaped DataFrame the
    # caller already produced via domain.scoring.quality_scorer; does not
    # recompute scoring here)
    # ------------------------------------------------------------------

    def _resolve_schema_quality_score(self) -> tuple[float | None, str | None]:
        df = self.quality_scores_df
        if df is None or df.empty or "Component" not in df.columns or "Value" not in df.columns:
            return None, None
        for component in _QUALITY_SCORE_COMPONENT_PRIORITY:
            rows = df[df["Component"] == component]
            if rows.empty:
                continue
            value = rows["Value"].iloc[0]
            try:
                if pd.notna(value):
                    return float(value), component
            except (TypeError, ValueError):
                continue
        return None, None
