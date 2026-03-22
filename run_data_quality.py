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
from dataquality.shared.telemetry import (
    TelemetryCollector,
    build_default_telemetry_path,
    clear_current_telemetry,
    set_current_telemetry,
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


def _parse_bool(raw_value: str | bool) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    value = str(raw_value).strip().lower()
    if value in {"1", "true", "yes", "on", "y"}:
        return True
    if value in {"0", "false", "no", "off", "n"}:
        return False
    raise ValueError(f"Invalid boolean value: {raw_value}")


def main() -> None:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config-json", default=None, type=str, help="Path to a JSON file with input arguments and validation settings.")
    bootstrap_args, _ = bootstrap.parse_known_args()
    template_config = build_data_quality_config_template()
    json_config = load_json_config(bootstrap_args.config_json)

    parser = argparse.ArgumentParser(description="Run Data Quality validations over CSV samples.")
    parser.add_argument("--config-json", default=None, type=str, help="Path to a JSON file with input arguments and validation settings.")
    parser.add_argument("--print-config-template", action="store_true", help="Print a JSON template with supported input arguments and exit.")
    parser.add_argument("--metadata-base-folder", default=get_config_value(json_config, "metadata_base_folder", template_config["metadata_base_folder"]), type=str, help="Folder with schema subfolders and metadados_*.csv files.")
    parser.add_argument("--sample-base-folder", default=get_config_value(json_config, "sample_base_folder", template_config["sample_base_folder"]), type=str, help="Folder with schema subfolders and sample CSV files per table.")
    parser.add_argument("--sample-source", default=get_config_value(json_config, "sample_source", template_config["sample_source"]), choices=["csv", "database"], help="Source for table samples used in data quality validation.")
    parser.add_argument("--db-connection-uri", default=get_config_value(json_config, "db_connection_uri", template_config.get("db_connection_uri")), type=str, help="Database connection URL or SQLAlchemy URI used when --sample-source database.")
    parser.add_argument("--db-authentication-type", default=get_config_value(json_config, "db_authentication_type", template_config["db_authentication_type"]), type=str, help="Authentication type for database access, e.g. username_password, external, iam.")
    parser.add_argument("--db-driver-class-name", default=get_config_value(json_config, "db_driver_class_name", template_config.get("db_driver_class_name")), type=str, help="Driver or dialect name. If the URL has no scheme, this prefix is used to compose it, e.g. oracle+oracledb.")
    parser.add_argument("--sample-query-template", default=get_config_value(json_config, "sample_query_template", template_config.get("sample_query_template")), type=str, help="Optional SQL template with placeholders {owner}, {table}, {limit}.")
    parser.add_argument("--sample-limit", default=get_config_value(json_config, "sample_limit", template_config["sample_limit"]), type=int, help="Maximum number of rows fetched per table when --sample-source database.")
    parser.add_argument("--telemetry-output", default=get_config_value(json_config, "telemetry_output", template_config.get("telemetry_output")), type=str, help="Optional path to write telemetry JSON for the execution.")
    parser.add_argument("--telemetry-enabled", default=get_config_value(json_config, "telemetry_enabled", template_config["telemetry_enabled"]), type=_parse_bool, help="Enable or disable telemetry JSON output. Use true/false.")
    parser.add_argument("--delete-cols", nargs="*", default=get_config_value(json_config, "delete_cols", template_config["delete_cols"]), help="Columns to drop after loading metadata.")
    parser.add_argument("--plural-exceptions", nargs="*", default=get_config_value(json_config, "plural_exceptions", template_config["plural_exceptions"]), help="Table names allowed to end with 'S'.")
    parser.add_argument("--db-type", default=get_config_value(json_config, "db_type", template_config["db_type"]), type=str, help="Database type for future compatibility.")
    parser.add_argument("--exclude-tables", nargs="*", default=get_config_value(json_config, "exclude_tables", template_config["exclude_tables"]), help="List of OWNER.TABLE or TABLE fragment to exclude.")
    args = parser.parse_args()
    if args.print_config_template:
        print(json.dumps(template_config, indent=2))
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
    if args.telemetry_enabled:
        telemetry_folder = Path(__file__).resolve().parent / "app"
        telemetry_path = Path(args.telemetry_output) if args.telemetry_output else build_default_telemetry_path(telemetry_folder, "telemetry_data_quality")
        collector = TelemetryCollector(run_name="data_quality", output_path=telemetry_path)
        collector.set_metadata(
            entrypoint="run_data_quality.py",
            sample_source=args.sample_source,
            db_type=args.db_type,
            metadata_base_folder=str(metadata_base_folder),
            sample_base_folder=str(sample_base_folder) if sample_base_folder is not None else None,
        )
        set_current_telemetry(collector)
    else:
        telemetry_path = None
        collector = None
        set_current_telemetry(None)
    try:
        run_data_quality(opts)
        if collector is not None:
            payload = collector.finalize("SUCCESS")
        else:
            payload = {"status": "SUCCESS"}
    except Exception:
        if collector is not None:
            payload = collector.finalize("FAILED")
        else:
            payload = {"status": "FAILED"}
        raise
    finally:
        clear_current_telemetry()
    if telemetry_path is not None:
        print("Telemetry saved to:", telemetry_path)
    else:
        print("Telemetry disabled")
    telemetry_status = payload.get("run_summary", {}).get("status", payload.get("status", "UNKNOWN"))
    print("Telemetry status:", telemetry_status)


if __name__ == "__main__":
    main()
