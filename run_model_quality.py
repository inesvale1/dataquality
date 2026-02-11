from __future__ import annotations

import argparse
from pathlib import Path

from dataquality.app.use_cases.run_model_quality import RunOptions, run_model_quality

def main() -> None:
    parser = argparse.ArgumentParser(description="Run Data Model Quality validations and metrics.")
    parser.add_argument("--base-folder", default="dataquality\\schema", type=str, help="Folder that contains schema subfolders with metadados_*.csv output files.")
    parser.add_argument("--delete-cols", nargs="*", default=["COLUMN_ID", "NUM_BUCKETS", "DENSITY"], help="Columns to drop after loading.")
    parser.add_argument("--plural-exceptions", nargs="*", default=["DAS","INS","SUBS","ICMS"], help="Table names allowed to end with 'S'.")
    parser.add_argument("--db-type", default="Oracle", type=str, help="Database type for DDL suggestions (e.g., Oracle).")
    parser.add_argument("--exclude-tables", nargs="*", default=["SUANOTA.NFP_DADOS_CADASTRAIS_HIST_BKP2", "MLOG$_"], help="List of OWNER.TABLE or TABLE fragment to exclude from validation/metrics.")
    args = parser.parse_args()  

    opts = RunOptions(
        base_folder=args.base_folder,
        columns_to_delete=args.delete_cols,
        plural_table_exceptions=args.plural_exceptions,
        db_type=args.db_type,
        exclude_tables=args.exclude_tables,
    )
    print("Saving to:", args.base_folder)

    run_model_quality(opts)


if __name__ == "__main__":
    main()
