from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class PipelineBridgeError(RuntimeError):
    pass


def ensure_metadata_context(
    schema_name: str,
    inputs_dir: Path,
    required: bool,
    workspace_root: Path,
) -> dict[str, Any]:
    """Return the schema's metadata_context, reading it from
    <inputs_dir>/metadata_context_<schema>.json.

    dataquality does not build this itself anymore -- that responsibility
    belongs to the sibling `technicalcatalogpipeline` project. When the file
    is missing:
    - if `required`, shell out to technicalcatalogpipeline to build it for
      just this schema, then re-read the file it wrote;
    - otherwise, return an empty context (tables=[], columns=[]) so callers
      (MetadataIssueSuggester etc.) degrade gracefully instead of failing.
    """
    context_path = inputs_dir / f"metadata_context_{schema_name}.json"
    if context_path.exists():
        return json.loads(context_path.read_text(encoding="utf-8"))

    if not required:
        return _empty_metadata_context(schema_name)

    pipeline_dir = workspace_root / "technicalcatalogpipeline"
    script = pipeline_dir / "scripts" / "build_technical_catalog.py"
    if not script.exists():
        raise PipelineBridgeError(
            f"require_metadata_context=true, mas {context_path} nao existe e "
            f"{script} tambem nao foi encontrado (esperado em {pipeline_dir})."
        )

    print(f"[pipeline_bridge] metadata_context_{schema_name}.json ausente; chamando technicalcatalogpipeline...")
    result = subprocess.run(
        [sys.executable, str(script), "--schema", schema_name],
        cwd=str(pipeline_dir),
        capture_output=True,
        text=True,
    )
    _print_subprocess_output("technicalcatalogpipeline", result)
    if result.returncode != 0 or not context_path.exists():
        raise PipelineBridgeError(
            f"technicalcatalogpipeline nao conseguiu gerar {context_path} "
            f"(exit code {result.returncode}). Veja a saida acima para o motivo "
            "(causa comum: metadata_{schema}.csv tambem nao existe em inputs/)."
        )

    return json.loads(context_path.read_text(encoding="utf-8"))


def ensure_sources_context(
    schema_name: str,
    inputs_dir: Path,
    required: bool,
    workspace_root: Path,
) -> dict[str, Any] | None:
    """Return the schema's sources_context (or None if unavailable/not
    required), reading it from <inputs_dir>/sources_context_<schema>.json.

    When `required` and the file is missing, shells out to the sibling
    `businessglossarypipeline` project to generate it. IMPORTANT: this makes a
    REAL LLM API call (via businessglossarypipeline's own
    config/catalogo.config.json + sources_repos.<schema>), which costs money
    and can take a while, and requires that schema's git repo to already be
    configured there. Keep require_sources_context=false unless you mean it.
    """
    context_path = inputs_dir / f"sources_context_{schema_name}.json"
    if context_path.exists():
        return json.loads(context_path.read_text(encoding="utf-8-sig"))

    if not required:
        return None

    pipeline_dir = workspace_root / "businessglossarypipeline"
    script = pipeline_dir / "scripts" / "generate_sources_context.py"
    if not script.exists():
        raise PipelineBridgeError(
            f"require_sources_context=true, mas {context_path} nao existe e "
            f"{script} tambem nao foi encontrado (esperado em {pipeline_dir})."
        )

    print(
        f"[pipeline_bridge] sources_context_{schema_name}.json ausente; chamando "
        "businessglossarypipeline (isso pode fazer uma chamada real a um LLM)..."
    )
    result = subprocess.run(
        [sys.executable, str(script), "--schema", schema_name],
        cwd=str(pipeline_dir),
        capture_output=True,
        text=True,
    )
    _print_subprocess_output("businessglossarypipeline", result)
    if result.returncode != 0 or not context_path.exists():
        raise PipelineBridgeError(
            f"businessglossarypipeline nao conseguiu gerar {context_path} "
            f"(exit code {result.returncode}). Veja a saida acima para o motivo "
            "(causa comum: sources_repos.{schema} nao configurado em "
            "businessglossarypipeline/config/catalogo.config.json)."
        )

    return json.loads(context_path.read_text(encoding="utf-8-sig"))


def ensure_business_docs_context(
    schema_name: str,
    inputs_dir: Path,
    required: bool,
    workspace_root: Path,
) -> dict[str, Any] | None:
    """Return the schema's business_docs_context (or None if unavailable/not
    required), reading it from <inputs_dir>/business_docs_context_<schema>.json.

    This is the vision/requirements/use-case/business-rule documents extracted
    from .odt/.docx/.pdf files (businessglossarypipeline's
    scripts/generate_sources_context.py --only docs), distinct from
    sources_context (extracted from Java/SQL source code).

    When `required` and the file is missing, shells out to the sibling
    `businessglossarypipeline` project with `--only docs` to generate it --
    this makes REAL LLM API calls, one per document (can be 100+), and
    requires that schema's sources_repos.<schema> to include at least one
    entry with content_type="docs". Keep required=false unless you mean it.
    """
    context_path = inputs_dir / f"business_docs_context_{schema_name}.json"
    if context_path.exists():
        return json.loads(context_path.read_text(encoding="utf-8-sig"))

    if not required:
        return None

    pipeline_dir = workspace_root / "businessglossarypipeline"
    script = pipeline_dir / "scripts" / "generate_sources_context.py"
    if not script.exists():
        raise PipelineBridgeError(
            f"required=true, mas {context_path} nao existe e "
            f"{script} tambem nao foi encontrado (esperado em {pipeline_dir})."
        )

    print(
        f"[pipeline_bridge] business_docs_context_{schema_name}.json ausente; chamando "
        "businessglossarypipeline --only docs (isso pode fazer varias chamadas reais a um LLM)..."
    )
    result = subprocess.run(
        [sys.executable, str(script), "--schema", schema_name, "--only", "docs"],
        cwd=str(pipeline_dir),
        capture_output=True,
        text=True,
    )
    _print_subprocess_output("businessglossarypipeline", result)
    if result.returncode != 0 or not context_path.exists():
        raise PipelineBridgeError(
            f"businessglossarypipeline nao conseguiu gerar {context_path} "
            f"(exit code {result.returncode}). Veja a saida acima para o motivo "
            "(causa comum: nenhum sources_repos.{schema} tem content_type=docs "
            "configurado em businessglossarypipeline/config/catalogo.config.json)."
        )

    return json.loads(context_path.read_text(encoding="utf-8-sig"))


def _empty_metadata_context(schema_name: str) -> dict[str, Any]:
    return {
        "schema_name": schema_name,
        "owner": schema_name.upper(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tables": [],
        "columns": [],
    }


def _print_subprocess_output(label: str, result: "subprocess.CompletedProcess[str]") -> None:
    if result.stdout.strip():
        print(f"[{label}] {result.stdout.strip()}")
    if result.stderr.strip():
        print(f"[{label}] {result.stderr.strip()}")
