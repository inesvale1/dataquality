from __future__ import annotations

import argparse
from pathlib import Path

from dataquality.app.use_cases.run_model_quality import RunOptions, run_model_quality

def main() -> None:
    parser = argparse.ArgumentParser(description="Run Data Model Quality validations and metrics.")
    parser.add_argument("--base-folder", default="dataquality\\schema", type=str, help="Folder that contains schema subfolders with metadados_*.csv output files.")
    parser.add_argument("--delete-cols", nargs="*", default=["COLUMN_ID", "NUM_BUCKETS", "DENSITY"], help="Columns to drop after loading.")
    parser.add_argument("--plural-exceptions", nargs="*", default=["DAS"], help="Table names allowed to end with 'S'.")
    args = parser.parse_args()  

    opts = RunOptions(
        base_folder=args.base_folder,
        columns_to_delete=args.delete_cols,
        plural_table_exceptions=args.plural_exceptions,
    )
    print("Saving to:", args.base_folder)

    run_model_quality(opts)


if __name__ == "__main__":
    main()
