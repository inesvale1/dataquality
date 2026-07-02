from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    package_parent = Path(__file__).resolve().parent.parent.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))

from dataquality.app.orchestration.denodo_catalog_input_builder import DenodoCatalogInputBuilder
from dataquality.infrastructure.io.csv.schema_loader import schemaLoader


def _load_schema_metadata(inputs_dir: Path, schema_name: str) -> pd.DataFrame:
    loader = schemaLoader(base_folder=inputs_dir)
    dictionary = loader.get_dictionary()
    if schema_name not in dictionary:
        raise FileNotFoundError(
            f"metadados_{schema_name}.csv not found under {inputs_dir} "
            f"(schemas found: {sorted(dictionary.keys()) or 'none'})"
        )
    return dictionary[schema_name]


def _load_business_context(inputs_dir: Path, schema_name: str) -> dict | None:
    context_path = inputs_dir / f"sources_context_{schema_name}.json"
    if not context_path.exists():
        return None
    return json.loads(context_path.read_text(encoding="utf-8-sig"))


def _load_quality_scores(excel_path: Path | None, sheet_name: str) -> pd.DataFrame | None:
    if not excel_path:
        return None
    return pd.read_excel(excel_path, sheet_name=sheet_name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a Denodo Data Catalog input JSON for one schema, reusing "
        "the technical metadata, the sources_context_<schema>.json business context, "
        "and (optionally) an already-computed METADATA_SCORES sheet."
    )
    parser.add_argument("--schema", required=True, help="Schema name, e.g. sitram2 or cadastro")
    parser.add_argument("--base-folder", default="schema", help="Root folder containing <schema>/inputs")
    parser.add_argument("--output-dir", default=None, help="Defaults to <base-folder>/<schema>/outputs")
    parser.add_argument(
        "--scores-excel",
        default=None,
        help="Optional path to an issues_metadados_*.xlsx report; its METADATA_SCORES "
        "sheet is reused to fill quality_score instead of recomputing it.",
    )
    parser.add_argument(
        "--scores-sheet",
        default="METADATA_SCORES",
        help="Sheet name holding the ScoreType/Component/Value rows (default: "
        "METADATA_SCORES; older reports may use QUALITY_SCORES with a different layout).",
    )
    args = parser.parse_args()

    base_folder = Path(args.base_folder)
    inputs_dir = base_folder / args.schema / "inputs"
    output_dir = Path(args.output_dir) if args.output_dir else base_folder / args.schema / "outputs"

    df_schema_metadata = _load_schema_metadata(inputs_dir, args.schema)
    business_context = _load_business_context(inputs_dir, args.schema)
    quality_scores_df = _load_quality_scores(
        Path(args.scores_excel) if args.scores_excel else None, args.scores_sheet
    )

    builder = DenodoCatalogInputBuilder(
        schema_name=args.schema,
        df_schema_metadata=df_schema_metadata,
        output_dir=output_dir,
        business_context=business_context,
        quality_scores_df=quality_scores_df,
    )
    output_path = builder.build_and_save()
    print(f"Denodo catalog input written to: {output_path}")
    if business_context is None:
        print(f"[warn] no sources_context_{args.schema}.json found under {inputs_dir}; "
              "business_glossary/business_context_candidates will be empty.")
    if quality_scores_df is None:
        print("[warn] no --scores-excel provided; quality_score will be null.")


if __name__ == "__main__":
    main()
