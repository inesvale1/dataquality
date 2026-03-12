from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


if __package__ in {None, ""}:
    package_parent = Path(__file__).resolve().parent.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))

from dataquality.app.use_cases.run_data_quality import RunDataQualityOptions, run_data_quality
from dataquality.shared.runtime_config import (
    build_data_quality_config_template,
    build_validation_config,
    get_config_value,
    load_json_config,
)


def _resolve_folder(raw_path: str, local_folder_name: str) -> Path:
    candidate = Path(raw_path)
    if candidate.exists():
        return candidate
    script_dir = Path(__file__).resolve().parent
    fallback = script_dir / local_folder_name
    if fallback.exists():
        return fallback
    return candidate


def main() -> None:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config-json", default=None, type=str, help="Path to a JSON file with input arguments and validation settings.")
    bootstrap_args, _ = bootstrap.parse_known_args()
    json_config = load_json_config(bootstrap_args.config_json)

    parser = argparse.ArgumentParser(description="Run Data Quality validations over CSV samples.")
    parser.add_argument("--config-json", default=None, type=str, help="Path to a JSON file with input arguments and validation settings.")
    parser.add_argument("--print-config-template", action="store_true", help="Print a JSON template with supported input arguments and exit.")
    parser.add_argument("--metadata-base-folder", default=get_config_value(json_config, "metadata_base_folder", "dataquality\\schema"), type=str, help="Folder with schema subfolders and metadados_*.csv files.")
    parser.add_argument("--sample-base-folder", default=get_config_value(json_config, "sample_base_folder", "dataquality\\samples"), type=str, help="Folder with schema subfolders and sample CSV files per table.")
    parser.add_argument("--sample-source", default=get_config_value(json_config, "sample_source", "csv"), choices=["csv", "database"], help="Source for table samples used in data quality validation.")
    parser.add_argument("--db-connection-uri", default=get_config_value(json_config, "db_connection_uri", None), type=str, help="Database connection URL or SQLAlchemy URI used when --sample-source database.")
    parser.add_argument("--db-authentication-type", default=get_config_value(json_config, "db_authentication_type", "username_password"), type=str, help="Authentication type for database access, e.g. username_password, external, iam.")
    parser.add_argument("--db-driver-class-name", default=get_config_value(json_config, "db_driver_class_name", None), type=str, help="Driver or dialect name. If the URL has no scheme, this prefix is used to compose it, e.g. oracle+oracledb.")
    parser.add_argument("--sample-query-template", default=get_config_value(json_config, "sample_query_template", None), type=str, help="Optional SQL template with placeholders {owner}, {table}, {limit}.")
    parser.add_argument("--sample-limit", default=get_config_value(json_config, "sample_limit", 1000), type=int, help="Maximum number of rows fetched per table when --sample-source database.")
    parser.add_argument("--delete-cols", nargs="*", default=get_config_value(json_config, "delete_cols", ["COLUMN_ID", "NUM_BUCKETS", "DENSITY"]), help="Columns to drop after loading metadata.")
    parser.add_argument("--plural-exceptions", nargs="*", default=get_config_value(json_config, "plural_exceptions", ["DAS", "INS", "SUBS", "ICMS"]), help="Table names allowed to end with 'S'.")
    parser.add_argument("--db-type", default=get_config_value(json_config, "db_type", "Oracle"), type=str, help="Database type for future compatibility.")
    parser.add_argument("--exclude-tables", nargs="*", default=get_config_value(json_config, "exclude_tables", ["RUPD$", "VW", "SUANOTA.NFP_DADOS_CADASTRAIS_HIST_BKP2", "MLOG$_"]), help="List of OWNER.TABLE or TABLE fragment to exclude.")
    args = parser.parse_args()
    if args.print_config_template:
        print(json.dumps(build_data_quality_config_template(), indent=2))
        return

    metadata_base_folder = _resolve_folder(args.metadata_base_folder, "schema")
    sample_base_folder = _resolve_folder(args.sample_base_folder, "samples") if args.sample_source == "csv" else None
    validation_config = build_validation_config(get_config_value(json_config, "validation_config", None))

    opts = RunDataQualityOptions(
        metadata_base_folder=metadata_base_folder,
        sample_base_folder=sample_base_folder,
        columns_to_delete=args.delete_cols,
        plural_table_exceptions=args.plural_exceptions,
        validation_config=validation_config,
        db_type=args.db_type,
        exclude_tables=args.exclude_tables,
        sample_source_type=args.sample_source,
        db_connection_uri=args.db_connection_uri,
        db_authentication_type=args.db_authentication_type,
        db_driver_class_name=args.db_driver_class_name,
        sample_query_template=args.sample_query_template,
        sample_limit=args.sample_limit,
    )
    print("Metadata folder:", metadata_base_folder)
    if sample_base_folder is not None:
        print("Sample folder:", sample_base_folder)
    print("Sample source:", args.sample_source)
    if args.sample_source == "database":
        print("Database type:", args.db_type)
        print("Authentication type:", args.db_authentication_type)
        print("Driver class name:", args.db_driver_class_name or "<from URL>")
    run_data_quality(opts)


if __name__ == "__main__":
    main()
