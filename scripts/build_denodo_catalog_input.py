from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    package_parent = Path(__file__).resolve().parent.parent.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))

from dataquality.app.orchestration.denodo_catalog_input_builder import DenodoCatalogInputBuilder


def _parse_bool(raw_value: str) -> bool:
    value = str(raw_value).strip().lower()
    if value in {"1", "true", "yes", "on", "y", "s", "verdade"}:
        return True
    if value in {"0", "false", "no", "off", "n", "falso"}:
        return False
    raise ValueError(f"Invalid boolean value: {raw_value}")


def _load_quality_scores(excel_path: Path | None, sheet_name: str) -> pd.DataFrame | None:
    if not excel_path:
        return None
    return pd.read_excel(excel_path, sheet_name=sheet_name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a Denodo Data Catalog input JSON for one schema, reusing "
        "the technical metadata_context, the sources_context_<schema>.json business context, "
        "and (optionally) an already-computed METADATA_SCORES sheet. Neither context is built "
        "by dataquality itself: if missing and --require-*-context is true, the sibling "
        "technicalcatalogpipeline / businessglossarypipeline projects are invoked to generate it "
        "(see dataquality/infrastructure/io/pipeline_bridge.py)."
    )
    parser.add_argument("--schema", required=True, help="Schema name, e.g. sitram2 or cadastro")
    parser.add_argument("--base-folder", default="../schema", help="Root folder containing <schema>/inputs")
    parser.add_argument("--output-dir", default=None, help="Defaults to <base-folder>/<schema>/outputs")
    parser.add_argument(
        "--scores-excel",
        default=None,
        help="Optional path to an issues_metadata_*.xlsx report; its METADATA_SCORES "
        "sheet is reused to fill quality_score instead of recomputing it.",
    )
    parser.add_argument(
        "--scores-sheet",
        default="METADATA_SCORES",
        help="Sheet name holding the ScoreType/Component/Value rows (default: "
        "METADATA_SCORES; older reports may use QUALITY_SCORES with a different layout).",
    )
    parser.add_argument(
        "--require-metadata-context",
        default=True,
        type=_parse_bool,
        help="If metadata_context_<schema>.json is missing, call technicalcatalogpipeline to build it (default: true).",
    )
    parser.add_argument(
        "--require-sources-context",
        default=False,
        type=_parse_bool,
        help="If sources_context_<schema>.json is missing, call businessglossarypipeline to build it "
        "-- this can make a REAL LLM API call and cost money/time (default: false).",
    )
    args = parser.parse_args()

    base_folder = Path(args.base_folder)
    inputs_dir = base_folder / args.schema / "inputs"
    output_dir = Path(args.output_dir) if args.output_dir else base_folder / args.schema / "outputs"
    # scripts/ -> dataquality/ -> Implementation/
    workspace_root = Path(__file__).resolve().parent.parent.parent

    quality_scores_df = _load_quality_scores(
        Path(args.scores_excel) if args.scores_excel else None, args.scores_sheet
    )

    builder = DenodoCatalogInputBuilder(
        schema_name=args.schema,
        inputs_dir=inputs_dir,
        output_dir=output_dir,
        workspace_root=workspace_root,
        require_metadata_context=args.require_metadata_context,
        require_sources_context=args.require_sources_context,
        quality_scores_df=quality_scores_df,
    )
    output_path = builder.build_and_save()
    print(f"Denodo catalog input written to: {output_path}")
    if quality_scores_df is None:
        print("[warn] no --scores-excel provided; quality_score will be null.")


if __name__ == "__main__":
    main()
