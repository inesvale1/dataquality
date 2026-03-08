from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    package_parent = Path(__file__).resolve().parent.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))

from dataquality.app.use_cases.run_model_quality import RunOptions, run_model_quality


def _resolve_base_folder(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if candidate.exists():
        return candidate
    script_dir = Path(__file__).resolve().parent
    fallback = script_dir / "schema"
    if candidate == Path("dataquality\\schema") and fallback.exists():
        return fallback
    return candidate

def main() -> None:
    parser = argparse.ArgumentParser(description="Run Data Model Quality validations and metrics.")
    parser.add_argument("--base-folder", default="dataquality\\schema", type=str, help="Folder that contains schema subfolders with metadados_*.csv output files.")
    parser.add_argument("--delete-cols", nargs="*", default=["COLUMN_ID", "NUM_BUCKETS", "DENSITY"], help="Columns to drop after loading.")
    parser.add_argument("--plural-exceptions", nargs="*", default=["DAS","INS","SUBS","ICMS"], help="Table names allowed to end with 'S'.")
    parser.add_argument("--db-type", default="Oracle", type=str, help="Database type for DDL suggestions (e.g., Oracle).")
    parser.add_argument("--exclude-tables", nargs="*", default=["SUANOTA.NFP_DADOS_CADASTRAIS_HIST_BKP2", "MLOG$_"], help="List of OWNER.TABLE or TABLE fragment to exclude from validation/metrics.")
    args = parser.parse_args()  
    base_folder = _resolve_base_folder(args.base_folder)

    opts = RunOptions(
        base_folder=base_folder,
        columns_to_delete=args.delete_cols,
        plural_table_exceptions=args.plural_exceptions,
        db_type=args.db_type,
        exclude_tables=args.exclude_tables,
    )
    print("Saving to:", base_folder)

    run_model_quality(opts)


if __name__ == "__main__":
    main()
